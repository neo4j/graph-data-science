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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.logging.NullLog;

import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

class GcListenerTest {

    @Test
    void testFilter() {
        NotificationFilter listener = new GcListener(
            NullLog.getInstance(),
            new AtomicLong(-1),
            new String[0],
            "some-name-42",
            MethodHandles.identity(Void.class)
        );
        Notification acceptedNotification = new Notification("some-name-42", "1337", 1337);
        assertTrue(listener.isNotificationEnabled(acceptedNotification));
        Notification ignoredNotification = new Notification("some-name-1337", "1337", 1337);
        assertFalse(listener.isNotificationEnabled(ignoredNotification));
    }

    @Test
    void testListenToEvent() throws OpenDataException {
        MemoryUsage heapUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long expectedFree = heapUsage.getMax() - heapUsage.getUsed();

        AtomicLong actualFree = new AtomicLong(-1);
        NotificationListener listener = new GcListener(
            NullLog.getInstance(),
            actualFree,
            new String[]{"global"},
            "42",
            testResultHandle("global", heapUsage)
        );

        Notification notification = new Notification("42", "1337", 1337);
        notification.setUserData(compositeData());
        listener.handleNotification(notification, null);

        assertEquals(expectedFree, actualFree.get());
    }

    private MethodHandle testResultHandle(Object... usagePairs) {
        Map<String, MemoryUsage> usages = MapUtil.genericMap(usagePairs);
        return dropArguments(
            constant(Map.class, usages),
            0,
            CompositeData.class
        );
    }

    @Test
    void testCombineMultipleUsages() throws OpenDataException {
        AtomicLong actualFree = new AtomicLong(-1);
        NotificationListener listener = new GcListener(
            NullLog.getInstance(),
            actualFree,
            new String[]{
                "usedWithMax",
                "usedWithoutMax",
                "unusedWithMax",
                "unusedWithoutMax",
            },
            "42",
            testResultHandle(
                "usedWithMax", new MemoryUsage(0, 42, 42, 1337),
                "usedWithoutMax", new MemoryUsage(0, 42, 42, -1),
                "unusedWithMax", new MemoryUsage(0, 0, 42, 1337),
                "unusedWithoutMax", new MemoryUsage(0, 0, 42, -1)
            )
        );

        Notification notification = new Notification("42", "1337", 1337);
        notification.setUserData(compositeData());
        listener.handleNotification(notification, null);

        long expectedMax = 1337 + 1337;
        long expectedUsed = 42 + 42;
        long expectedFree = expectedMax - expectedUsed;

        assertEquals(expectedFree, actualFree.get());
    }

    private CompositeData compositeData() throws OpenDataException {
        CompositeType compositeType = new CompositeType(
            "String",
            "String",
            new String[]{"foo"},
            new String[]{"foo"},
            new OpenType[]{SimpleType.STRING}
        );
        return new CompositeDataSupport(
            compositeType,
            singletonMap("foo", "foo")
        );
    }
}
