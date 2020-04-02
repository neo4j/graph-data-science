/*
 * Copyright (c) 2017-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.annotation.IdenticalCompat;
import org.neo4j.logging.LogProvider;

import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.invoke.MethodHandles.filterReturnValue;

@IdenticalCompat
final class HotSpotGcListener {
    private static final boolean ENABLED;
    private static final String GC_NOTIFICATION_NAME;
    private static final MethodHandle GET_USAGE_AFTER_GC;

    static Optional<NotificationListener> install(
        LogProvider logProvider,
        AtomicLong freeMemory,
        String[] poolNames,
        NotificationBroadcaster broadcaster
    ) {
        if (ENABLED) {
            GcListener listener = new GcListener(
                logProvider.getLog(GcListener.class),
                freeMemory,
                poolNames,
                GC_NOTIFICATION_NAME,
                GET_USAGE_AFTER_GC
            );
            broadcaster.addNotificationListener(listener, listener, null);
            return Optional.of(listener);
        }
        return Optional.empty();
    }

    static {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        String gcNotificationName = null;
        MethodHandle getUsageAfterGc = null;
        boolean enabled = false;
        try {
            // Implementing the listener requires calling private HotSpot methods in the com.sun package.
            // They are not part of the OpenJDK and we want to be good citizens,
            //  so we are installing the listener only if we can access the required methods reflectively.
            // On different VMs, we don't install any listener. We would then never change the initial
            //  value of free memory, which is the max available heap and therefore allow anything that fits.

            Class<?> gcNotifyClass = Class.forName("com.sun.management.GarbageCollectionNotificationInfo");
            MethodHandle notificationNameField = lookup.findStaticGetter(
                gcNotifyClass,
                "GARBAGE_COLLECTION_NOTIFICATION",
                String.class
            );

            try {
                // reading: GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                gcNotificationName = String.valueOf(notificationNameField.invoke());
            } catch (Throwable ignored) {
                // if we can't find the constant, we inline the default from the
                //  HotSpot class. If that changes at some point, i.e. the constant is gone,
                //  we will very likely fail on creating the following method handles anyway.
                gcNotificationName = "com.sun.management.gc.notification";
            }

            // calling static method: GarbageCollectionNotificationInfo.from(CompositeData userData)
            MethodHandle infoMethod = lookup.findStatic(
                gcNotifyClass,
                "from",
                MethodType.methodType(gcNotifyClass, CompositeData.class)
            );

            Class<?> gcInfoClass = Class.forName("com.sun.management.GcInfo");
            // calling instance method: GcInfo GarbageCollectionNotificationInfo#getGcInfo()
            MethodHandle getGcInfo = lookup.findVirtual(
                gcNotifyClass,
                "getGcInfo",
                MethodType.methodType(gcInfoClass)
            );

            // equivalent to: GcInfo info = GarbageCollectionNotificationInfo.from(userData).getGcInfo();
            getGcInfo = filterReturnValue(infoMethod, getGcInfo);

            // calling instance method: Map<String, MemoryUsage> GcInfo#getMemoryUsageAfterGc()
            MethodHandle getMemoryUsageAfterGc = lookup.findVirtual(
                gcInfoClass,
                "getMemoryUsageAfterGc",
                MethodType.methodType(Map.class)
            );

            // equivalent to:
            //   GcInfo info = GarbageCollectionNotificationInfo.from(userData).getGcInfo();
            //   Map<String, MemoryUsage> afterGc = info.getMemoryUsageAfterGc()
            getMemoryUsageAfterGc = filterReturnValue(getGcInfo, getMemoryUsageAfterGc);
            getUsageAfterGc = getMemoryUsageAfterGc;

            enabled = true;
        } catch (NoSuchMethodException | IllegalAccessException | NoSuchFieldException | ClassNotFoundException ignored) {
            // The class internals have changed or we are running on a non-HotSpot VM.
            // Instead of crashing the DB, we will not run in that case and just
            //  keep the initial value for free memory.
        }
        GC_NOTIFICATION_NAME = gcNotificationName;
        GET_USAGE_AFTER_GC = getUsageAfterGc;
        ENABLED = enabled;
    }

    private HotSpotGcListener() {}
}
