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


