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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.compat.MemoryTrackerProxy;
import org.neo4j.graphalgo.utils.CheckedRunnable;
import org.neo4j.util.FeatureToggles;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

public interface AllocationTracker extends Supplier<String> {

    AllocationTracker EMPTY = EmptyAllocationTracker.INSTANCE;

    static boolean isTracking(@Nullable AllocationTracker tracker) {
        return tracker != null && tracker != EMPTY;
    }

    AtomicBoolean USE_KERNEL_TRACKER = new AtomicBoolean(FeatureToggles.flag(
        AllocationTracker.class,
        "useKernelTracker",
        false
    ));

    static boolean useKernelTracker() {
        return USE_KERNEL_TRACKER.get();
    }

    static AllocationTracker create() {
        return InMemoryAllocationTracker.create();
    }

    static AllocationTracker create(MemoryTrackerProxy kernelProxy) {
        return kernelProxy
            .fold(
                InMemoryAllocationTracker::create,
                () -> AllocationTracker.EMPTY,
                useKernelTracker() ? KernelAllocationTracker::create : InMemoryAllocationTracker::ignoring
            );
    }

    @TestOnly
    static <E extends Exception> void whileUsingKernelTracker(CheckedRunnable<E> code) throws E {
        var useKernelTracker = USE_KERNEL_TRACKER.getAndSet(true);
        try {
            code.checkedRun();
        } finally {
            USE_KERNEL_TRACKER.set(useKernelTracker);
        }
    }

    /**
     * Add the given number of bytes to the total tracked amount.
     */
    void add(long bytes);

    /**
     * Remove the given number of bytes from the total tracked amount.
     */
    void remove(long bytes);

    /**
     * Return the current total of tracked bytes.
     */
    long trackedBytes();

    default String getUsageString() {
        return humanReadable(trackedBytes());
    }

    default String getUsageString(String label) {
        return label + getUsageString();
    }

    @Override
    default String get() {
        return getUsageString("Memory usage: ");
    }

}
