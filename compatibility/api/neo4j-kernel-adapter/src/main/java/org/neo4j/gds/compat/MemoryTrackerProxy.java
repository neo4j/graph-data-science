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
package org.neo4j.gds.compat;

import java.util.function.Function;
import java.util.function.Supplier;

public interface MemoryTrackerProxy {

    <R> R fold(
        Supplier<R> onUnsupported,
        Supplier<R> onEmpty,
        Function<AllocationTrackerAdapter, R> onSupported
    );

    MemoryTrackerProxy UNSUPPORTED = new UnsupportedMemoryTrackerProxy();

    MemoryTrackerProxy EMPTY = new EmptyMemoryTrackerProxy();
}

final class UnsupportedMemoryTrackerProxy implements MemoryTrackerProxy {
    @Override
    public <R> R fold(
        Supplier<R> onUnsupported,
        Supplier<R> onEmpty,
        Function<AllocationTrackerAdapter, R> onSupported
    ) {
        return onUnsupported.get();
    }
}

final class EmptyMemoryTrackerProxy implements MemoryTrackerProxy {
    @Override
    public <R> R fold(
        Supplier<R> onUnsupported,
        Supplier<R> onEmpty,
        Function<AllocationTrackerAdapter, R> onSupported
    ) {
        return onEmpty.get();
    }
}
