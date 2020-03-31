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
import org.neo4j.logging.Log;

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.invoke.MethodHandle;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@IdenticalCompat
final class GcListener implements NotificationListener, NotificationFilter {
    // NotificationFilter is Serializable
    private static final long serialVersionUID = 133742;

    private final Log log;
    private final AtomicLong freeMemory;
    private final String[] poolNames;
    private final String gcNotificationName;
    private final MethodHandle getMemoryUsage;
    private final AtomicBoolean reflectionWarningEmitted;

    GcListener(
        Log log,
        AtomicLong freeMemory,
        String[] poolNames,
        String gcNotificationName,
        MethodHandle getMemoryUsage
    ) {
        this.log = log;
        this.freeMemory = freeMemory;
        this.poolNames = poolNames;
        this.gcNotificationName = gcNotificationName;
        this.getMemoryUsage = getMemoryUsage;
        this.reflectionWarningEmitted = new AtomicBoolean(false);
    }

    @Override
    public boolean isNotificationEnabled(Notification notification) {
        return notification.getType().equals(gcNotificationName);
    }

    @Override
    public void handleNotification(Notification notification, Object handback) {
        Object notificationData = notification.getUserData();
        if (!(notificationData instanceof CompositeData)) {
            return;
        }
        CompositeData userData = (CompositeData) notificationData;

        // The following map contains detailed memory information about not just the various heap spaces,
        //  but also about JVM internal pools like Metaspace or JIT-compiled code.
        // We have no control over those spaces, but they will use some available heap.
        // We explicitly ignore those pools and only look at the usages from the collected pools.
        // As a result, we will see more free memory than there probably is.
        // It also depends on the GC on whether or not those internal pools take space from the user heap.
        // We want to err on the side of allowing things that might actually fail
        //  instead of blocking things that could succeed,
        //  and under this assumption only taking the collected pools into account is correct.

        Map<String, MemoryUsage> afterGc = null;
        try {
            @SuppressWarnings("unchecked") Map<String, MemoryUsage> usage =
                (Map<String, MemoryUsage>) getMemoryUsage.invoke(userData);
            afterGc = usage;
        } catch (Throwable throwable) {
            // If we get an error while calling the MethodHandle, we will very likely
            //  get a similar error on all following invocations.
            // To prevent flooding the log with the same error message, we only log once per listener.
            if (this.reflectionWarningEmitted.compareAndSet(false, true)) {
                log.warn("Could not convert the notification data into a memory usage map", throwable);
            }
        }

        // Could be null if we have an error while calling the MethodHandle or if it actually returns null.
        if (afterGc == null) {
            return;
        }

        long freeAfterGc = 0L;
        for (String poolName : this.poolNames) {
            MemoryUsage usageAfterGc = afterGc.get(poolName);
            if (usageAfterGc != null) {
                long maxPoolSize = usageAfterGc.getMax();
                // If the max size is -1, the pool is a variable sized subsection of some other pool and has no
                // own size. This is true, for example, for the Spaces Eden and Survivor of the G1 GC.
                // G1 has a single heap size, that of the Old Space, and Eden and Survivor use some regions of Old
                // for their data. How many regions is adjusted dynamically by G1 to try to make
                // collection pause times adhere to the target max pause time.
                // We still need to subtract the used space though.
                if (maxPoolSize != -1L) {
                    freeAfterGc += maxPoolSize;
                }
                freeAfterGc -= usageAfterGc.getUsed();
            }
        }

        log.debug("Free pool memory after GC: %d", freeAfterGc);
        this.freeMemory.set(freeAfterGc);
    }
}
