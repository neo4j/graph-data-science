/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.utils.mem;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.io.ByteUnit;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.GdsFeatureToggles.USE_KERNEL_TRACKER;

class AllocationTrackerTest {

    private static final long GRAB_SIZE_1KB = ByteUnit.kibiBytes(1);
    private static final String EXCEPTION_NAME = "MemoryLimitExceededException";

    @ParameterizedTest
    @MethodSource("allocationTrackers")
    void testAddForInMemoryTracking() {
        var allocationTracker = AllocationTracker.create();
        allocationTracker.add(42);
        assertEquals(42, allocationTracker.trackedBytes());
    }

    @ParameterizedTest
    @MethodSource("allocationTrackers")
    void testRemoveForInMemoryTracking() {
        var allocationTracker = AllocationTracker.create();
        allocationTracker.add(1337);
        allocationTracker.remove(42);
        assertEquals(1337 - 42, allocationTracker.trackedBytes());
    }

    @Test
    void testStringOutputForInMemoryTracking() {
        var allocationTracker = AllocationTracker.create();
        allocationTracker.add(1337);
        assertEquals("1337 Bytes", allocationTracker.getUsageString());
        allocationTracker.add(1337 * 42);
        assertEquals("56 KiB", allocationTracker.getUsageString());
    }

    @ParameterizedTest
    @MethodSource("emptyTrackers")
    void testAddForEmptyTracking(AllocationTracker allocationTracker) {
        allocationTracker.add(1337);
        assertEquals(0, allocationTracker.trackedBytes());
        allocationTracker.remove(42);
        assertEquals(0, allocationTracker.trackedBytes());
    }

    @Test
    void testCheckForAllocationTracking() {
        var allocationTracker = AllocationTracker.create();
        assertTrue(AllocationTracker.isTracking(allocationTracker));
        assertFalse(AllocationTracker.isTracking(AllocationTracker.empty()));
        assertFalse(AllocationTracker.isTracking(null));
    }

    @Test
    void shouldUseInMemoryTrackerWhenFeatureIsToggledOff() {
        var memoryTrackerProxy = Neo4jProxy.limitedMemoryTrackerProxy(42, GRAB_SIZE_1KB);
        var allocationTracker = AllocationTracker.create(memoryTrackerProxy);
        assertThat(allocationTracker).isExactlyInstanceOf(InMemoryAllocationTracker.class);
    }

    @Test
    void shouldUseKernelTrackerWhenFeatureIsToggledOn() {
        USE_KERNEL_TRACKER.enableAndRun(() -> {
            var memoryTrackerProxy = Neo4jProxy.limitedMemoryTrackerProxy(1337, GRAB_SIZE_1KB);
            var allocationTracker = AllocationTracker.create(memoryTrackerProxy);
            assertThat(allocationTracker).isExactlyInstanceOf(KernelAllocationTracker.class);
        });
    }

    @Test
    void shouldTerminateTransactionWhenOverallocating() {
        USE_KERNEL_TRACKER.enableAndRun(
            () -> {
                var memoryTrackerProxy = Neo4jProxy.limitedMemoryTrackerProxy(42, GRAB_SIZE_1KB);
                var allocationTracker = AllocationTracker.create(memoryTrackerProxy);
                allocationTracker.add(42);
                assertEquals(42L, allocationTracker.trackedBytes());
                var exception = assertThrows(
                    Exception.class,
                    () -> allocationTracker.add(1)
                );

                assertThat(exception.getClass().getSimpleName()).isEqualTo(EXCEPTION_NAME);
                assertThat(exception).hasMessageStartingWith("The allocation of an extra 1 B would use more than the limit 42 B.");
            }
        );
    }

    static Stream<AllocationTracker> emptyTrackers() {
        return Stream.of(
            AllocationTracker.empty(),
            AllocationTracker.create(Neo4jProxy.emptyMemoryTrackerProxy())
        );
    }

    static Stream<AllocationTracker> allocationTrackers() {
        return Stream.of(
            AllocationTracker.create(),
            AllocationTracker.create(Neo4jProxy.limitedMemoryTrackerProxy(Long.MAX_VALUE, GRAB_SIZE_1KB))
        );
    }
}
