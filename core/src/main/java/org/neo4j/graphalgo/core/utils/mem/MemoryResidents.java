/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.core.GraphDimensions;

import java.util.function.IntToLongFunction;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

final class MemoryResidents {

    static MemoryResident empty() {
        return NULL_RESIDENT;
    }

    static MemoryResident fixed(final MemoryRange range) {
        return (dimensions, concurrecny) -> range;
    }

    static MemoryResident perNode(final MemoryRange range) {
        return (dimensions, concurrecny) -> range.times(dimensions.hugeNodeCount());
    }

    static MemoryResident perNode(final LongUnaryOperator fn) {
        return (dimensions, concurrecny) -> MemoryRange.of(fn.applyAsLong(dimensions.hugeNodeCount()));
    }

    static MemoryResident perNode(final LongFunction<MemoryRange> fn) {
        return (dimensions, concurrecny) -> fn.apply(dimensions.hugeNodeCount());
    }

    static MemoryResident perDim(final ToLongFunction<GraphDimensions> fn) {
        return (dimensions, concurrecny) -> MemoryRange.of(fn.applyAsLong(dimensions));
    }

    static MemoryResident perThread(final MemoryRange range) {
        return (dimensions, concurrecny) -> range.times(concurrecny);
    }

    static MemoryResident perThread(final IntToLongFunction fn) {
        return (dimensions, concurrecny) -> MemoryRange.of(fn.applyAsLong(concurrecny));
    }

    static MemoryResident composite(final Iterable<MemoryResident> components) {
        return (dimensions, concurrecny) -> {
            MemoryRange range = MemoryRange.empty();
            for (MemoryResident component : components) {
                range = range.add(component.estimateMemoryUsage(dimensions, concurrecny));
            }
            return range;
        };
    }

    private static final MemoryResident NULL_RESIDENT = (dimensions, concurrecny) -> MemoryRange.empty();

    private MemoryResidents() {
        throw new UnsupportedOperationException("No instances");
    }
}


