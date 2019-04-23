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
package org.neo4j.graphalgo.api;


import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

/**
 * Composition of often used source interfaces
 *
 * @author mknblch
 */
public interface HugeGraph extends HugeIdMapping, HugeDegrees, HugeNodeIterator, HugeBatchNodeIterable, HugeRelationshipIterator, HugeRelationshipWeights, HugeRelationshipPredicate, HugeRelationshipAccess, HugeNodeProperties, Graph {

    String TYPE = "huge";

    /**
     * release resources which are not part of the result or IdMapping
     */
    default void release() {

    }

    default String getType() {
        return TYPE;
    }

    @Override
    default Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return hugeBatchIterables(batchSize)
                .stream()
                .map(l -> (PrimitiveIntIterable) () -> new LongToIntIterator(l.iterator()))
                .collect(Collectors.toList());
    }

    @Override
    default int degree(int nodeId, Direction direction) {
        return degree((long) nodeId, direction);
    }

    @Override
    default int toMappedNodeId(long nodeId) {
        return (int) toHugeMappedNodeId(nodeId);
    }

    @Override
    default long toOriginalNodeId(int nodeId) {
        return toOriginalNodeId((long) nodeId);
    }

    @Override
    default void forEachNode(IntPredicate consumer) {
        forEachNode((LongPredicate) l -> consumer.test((int) l));
    }

    @Override
    default PrimitiveIntIterator nodeIterator() {
        return new LongToIntIterator(hugeNodeIterator());
    }

    @Override
    default double weightOf(
            int sourceNodeId,
            int targetNodeId) {
        return weightOf((long) sourceNodeId, (long) targetNodeId);
    }

    final class LongToIntIterator implements PrimitiveIntIterator {
        private final PrimitiveLongIterator iter;

        LongToIntIterator(final PrimitiveLongIterator iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public int next() {
            return (int) iter.next();
        }
    }
}
