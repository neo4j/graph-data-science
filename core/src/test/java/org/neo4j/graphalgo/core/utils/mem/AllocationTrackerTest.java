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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.io.ByteUnit;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AllocationTrackerTest {

    private static final long GRAB_SIZE_1KB = ByteUnit.kibiBytes(1);

    @ParameterizedTest
    @MethodSource("allocationTrackers")
    void testAddForInMemoryTracking() {
        var tracker = AllocationTracker.create();
        tracker.add(42);
        assertEquals(42, tracker.trackedBytes());
    }

    @ParameterizedTest
    @MethodSource("allocationTrackers")
    void testRemoveForInMemoryTracking() {
        var tracker = AllocationTracker.create();
        tracker.add(1337);
        tracker.remove(42);
        assertEquals(1337 - 42, tracker.trackedBytes());
    }

    @Test
    void testStringOutputForInMemoryTracking() {
        var tracker = AllocationTracker.create();
        tracker.add(1337);
        assertEquals("1337 Bytes", tracker.getUsageString());
        tracker.add(1337 * 42);
        assertEquals("56 KiB", tracker.getUsageString());
    }

    @ParameterizedTest
    @MethodSource("emptyTrackers")
    void testAddForEmptyTracking(AllocationTracker tracker) {
        tracker.add(1337);
        assertEquals(0, tracker.trackedBytes());
        tracker.remove(42);
        assertEquals(0, tracker.trackedBytes());
    }

    @Test
    void testCheckForAllocationTracking() {
        var tracker = AllocationTracker.create();
        assertTrue(AllocationTracker.isTracking(tracker));
        assertFalse(AllocationTracker.isTracking(AllocationTracker.EMPTY));
        assertFalse(AllocationTracker.isTracking(null));
    }

    @Test
    void shouldUseInMemoryTrackerWhenFeatureIsToggledOff() {
        var memoryTracker = Neo4jProxy.limitedMemoryTracker(42, GRAB_SIZE_1KB);
        var trackerProxy = Neo4jProxy.memoryTrackerProxy(memoryTracker);
        var allocationTracker = AllocationTracker.create(trackerProxy);
        assertThat(allocationTracker).isExactlyInstanceOf(InMemoryAllocationTracker.class);
    }

    @Test
    void shouldUseKernelTrackerWhenFeatureIsToggledOn() {
        // There is no KernelTracker in 4.0
        Assumptions.assumeFalse(is40());
        AllocationTracker.whileUsingKernelTracker(
            () -> {
                var memoryTracker = Neo4jProxy.limitedMemoryTracker(1337, GRAB_SIZE_1KB);
                var trackerProxy = Neo4jProxy.memoryTrackerProxy(memoryTracker);
                var allocationTracker = AllocationTracker.create(trackerProxy);
                assertThat(allocationTracker).isExactlyInstanceOf(KernelAllocationTracker.class);
            }
        );
    }

    @Test
    void shouldIgnoreFeatureToggleOn40() {
        // There is no KernelTracker in 4.0
        Assumptions.assumeTrue(is40());
        AllocationTracker.whileUsingKernelTracker(
            () -> {
                var memoryTracker = Neo4jProxy.limitedMemoryTracker(1337, GRAB_SIZE_1KB);
                var trackerProxy = Neo4jProxy.memoryTrackerProxy(memoryTracker);
                var allocationTracker = AllocationTracker.create(trackerProxy);
                assertThat(allocationTracker).isExactlyInstanceOf(InMemoryAllocationTracker.class);
            }
        );
    }

    private static boolean is40() {
        return GraphDatabaseApiProxy.neo4jVersion() == GraphDatabaseApiProxy.Neo4jVersion.V_4_0;
    }

    static Stream<AllocationTracker> emptyTrackers() {
        return Stream.concat(
            Stream.of(AllocationTracker.EMPTY),
            is40()
                ? Stream.empty()
                : Stream.of(AllocationTracker.create(Neo4jProxy.memoryTrackerProxy(Neo4jProxy.emptyMemoryTracker())))
        );
    }

    static Stream<AllocationTracker> allocationTrackers() {
        return Stream.of(
            AllocationTracker.create(),
            AllocationTracker.create(Neo4jProxy.memoryTrackerProxy(Neo4jProxy.limitedMemoryTracker(Long.MAX_VALUE, GRAB_SIZE_1KB)))
        );
    }
}
