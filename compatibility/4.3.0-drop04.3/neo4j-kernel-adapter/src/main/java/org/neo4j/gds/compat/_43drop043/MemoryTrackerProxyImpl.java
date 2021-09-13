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
package org.neo4j.gds.compat._43drop043;

import org.neo4j.gds.compat.AllocationTrackerAdapter;
import org.neo4j.gds.compat.MemoryTrackerProxy;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import java.util.function.Function;
import java.util.function.Supplier;

final class MemoryTrackerProxyImpl implements MemoryTrackerProxy {

    private final AllocationTrackerAdapter allocationTracker;

    static MemoryTrackerProxy of(MemoryTracker memoryTracker) {
        if (memoryTracker instanceof EmptyMemoryTracker) {
            return MemoryTrackerProxy.EMPTY;
        }
        return new MemoryTrackerProxyImpl(new AllocationTrackerAdapterImpl(memoryTracker));
    }

    private MemoryTrackerProxyImpl(AllocationTrackerAdapter allocationTracker) {
        this.allocationTracker = allocationTracker;
    }

    @Override
    public <R> R fold(
        Supplier<R> onUnsupported,
        Supplier<R> onEmpty,
        Function<AllocationTrackerAdapter, R> onSupported
    ) {
        return onSupported.apply(allocationTracker);
    }
}
