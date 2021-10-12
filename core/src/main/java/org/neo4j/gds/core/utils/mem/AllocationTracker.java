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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.compat.MemoryTrackerProxy;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.function.Supplier;

public interface AllocationTracker extends Supplier<String> {

    static AllocationTracker empty() {
        return EmptyAllocationTracker.INSTANCE;
    }

    static boolean isTracking(@Nullable AllocationTracker allocationTracker) {
        return allocationTracker != null && allocationTracker != empty();
    }

    static AllocationTracker create() {
        return InMemoryAllocationTracker.create();
    }

    static AllocationTracker create(MemoryTrackerProxy kernelProxy) {
        return kernelProxy
            .fold(
                AllocationTracker::create,
                AllocationTracker::empty,
                GdsFeatureToggles.USE_KERNEL_TRACKER.isEnabled() ? KernelAllocationTracker::create : InMemoryAllocationTracker::ignoring
            );
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
        return MemoryUsage.humanReadable(trackedBytes());
    }

    default String getUsageString(String label) {
        return label + getUsageString();
    }

    @Override
    default String get() {
        return getUsageString("Memory usage: ");
    }

}
