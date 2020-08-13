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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;

public abstract class AllocationTracker implements Supplier<String> {

    public static final AllocationTracker EMPTY = new AllocationTracker() {
        @Override
        public void add(long delta) {
        }

        @Override
        public void remove(long delta) {
        }

        @Override
        public long tracked() {
            return 0L;
        }

        @Override
        public String get() {
            return "";
        }

        @Override
        public String getUsageString() {
            return "";
        }

        @Override
        public String getUsageString(String label) {
            return "";
        }
    };

    public static boolean isTracking(@Nullable AllocationTracker tracker) {
        return tracker != null && tracker != EMPTY;
    }

    private static final AtomicBoolean USE_KERNEL_TRACKER = new AtomicBoolean(FeatureToggles.flag(
        AllocationTracker.class,
        "useKernelTracker",
        false
    ));

    public static boolean useKernelTracker() {
        return USE_KERNEL_TRACKER.get();
    }

    static void useKernelTracker(boolean value) {
        USE_KERNEL_TRACKER.set(value);
    }

    public static AllocationTracker create() {
        return create(Optional.empty());
    }

    public static AllocationTracker create(Optional<MemoryTrackerProxy> kernelProxy) {
        return kernelProxy
            .filter(ignore -> useKernelTracker())
            .map(KernelAllocationTracker::create)
            .orElseGet(InMemoryAllocationTracker::create);
    }

    @TestOnly
    public static synchronized <E extends Exception> void whileUsingKernelTracker(CheckedRunnable<E> code) throws E {
        var useKernelTracker = USE_KERNEL_TRACKER.getAndSet(true);
        try {
            code.checkedRun();
        } finally {
            USE_KERNEL_TRACKER.set(useKernelTracker);
        }
    }

    public abstract void add(long delta);

    public abstract void remove(long delta);

    public abstract long tracked();

    public String getUsageString() {
        return humanReadable(tracked());
    }

    public String getUsageString(String label) {
        return label + getUsageString();
    }

    @Override
    public String get() {
        return getUsageString("Memory usage: ");
    }
}
