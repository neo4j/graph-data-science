package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.IntBinaryPredicate;
import org.neo4j.graphalgo.api.RelationshipConsumer;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/**
 * Warning: These conversions are not safe but will fail for very large graphs.
 * These are to be used by algorithms that migrate to the new SPI but are based on integers.
 * The same limitations apply for those algorithms as before, but failures for very large graphs will be contained in here.
 */
public interface Converters {

    static LongPredicate longToIntPredicate(IntPredicate p) {
        return value -> {
            // This will fail on very large graphs
            int downCast = Math.toIntExact(value);
            return p.test(downCast);
        };
    }

    static LongConsumer longToIntConsumer(IntConsumer p) {
        return value -> {
            // This will fail on very large graphs
            int downCast = Math.toIntExact(value);
            p.accept(downCast);
        };
    }

    static RelationshipConsumer longToIntConsumer(IntBinaryPredicate p) {
        return (sourceNodeId, targetNodeId) -> {
            int s = Math.toIntExact(sourceNodeId);
            int t = Math.toIntExact(targetNodeId);
            return p.test(s, t);
        };
    }
}
