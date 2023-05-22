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
package org.neo4j.gds.core.utils.partition;

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.gds.mem.BitUtil;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class LazyDegreePartitionIterator extends AbstractIterator<DegreePartition> {

    abstract Stream<DegreePartition> stream();

    static LazyDegreePartitionIterator of(
        long nodeCount,
        long relationshipCount,
        int concurrency,
        PartitionUtils.DegreeFunction degrees
    ) {
        if (concurrency == 1) {
            return new SingleDegreePartitionIterator(nodeCount, relationshipCount);
        }

        return new MultiDegreePartitionIterator(nodeCount, relationshipCount, concurrency, degrees);
    }

    private final static class MultiDegreePartitionIterator extends LazyDegreePartitionIterator {

        private static final long DIVISION_FACTOR = 10;

        private final long nodeCount;
        private final PartitionUtils.DegreeFunction degrees;
        private long currentStartNode;
        private final long numRelationshipsInPartition;

        MultiDegreePartitionIterator(
            long nodeCount,
            long relationshipCount,
            long concurrency,
            PartitionUtils.DegreeFunction degrees
        ) {
            this.nodeCount = nodeCount;
            this.degrees = degrees;
            this.currentStartNode = 0;
            this.numRelationshipsInPartition = BitUtil.ceilDiv(relationshipCount, concurrency * DIVISION_FACTOR);
        }

        @Override
        protected DegreePartition fetch() {
            long nodeId = this.currentStartNode;

            if (nodeId >= nodeCount) {
                return done();
            }

            long currentRelationshipCount = 0;
            long startNode = nodeId;

            for (; nodeId < nodeCount; nodeId++) {
                long nextRelationshipCount = degrees.degree(nodeId);
                if (currentRelationshipCount + nextRelationshipCount > numRelationshipsInPartition) {
                    this.currentStartNode = nodeId + 1;
                    return DegreePartition.of(
                        startNode,
                        currentStartNode - startNode,
                        currentRelationshipCount + nextRelationshipCount
                    );
                }
                currentRelationshipCount += nextRelationshipCount;
            }

            this.currentStartNode = nodeId + 1;
            return DegreePartition.of(startNode, nodeCount - startNode, currentRelationshipCount);
        }

        public Stream<DegreePartition> stream() {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                this,
                Spliterator.ORDERED
                    | Spliterator.DISTINCT
                    | Spliterator.SORTED
                    | Spliterator.IMMUTABLE
            ), false);
        }

    }

    private static final class SingleDegreePartitionIterator extends LazyDegreePartitionIterator {

        private final long nodeCount;
        private final long relationshipCount;

        private boolean calledBefore;

        SingleDegreePartitionIterator(
            long nodeCount,
            long relationshipCount
        ) {
            this.relationshipCount = relationshipCount;
            this.nodeCount = nodeCount;
            this.calledBefore = false;
        }

        @Override
        protected DegreePartition fetch() {
            if (!calledBefore) {
                this.calledBefore = true;
                return DegreePartition.of(0, nodeCount, relationshipCount);
            } else {
                return done();
            }
        }

        @Override
        Stream<DegreePartition> stream() {
            return Stream.of(fetch());
        }
    }
}
