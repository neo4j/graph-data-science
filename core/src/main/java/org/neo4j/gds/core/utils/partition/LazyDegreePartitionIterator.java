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

    // This is a good guess to achieve smaller partitions
    private static final long DIVISION_FACTOR = 10;

    abstract Stream<DegreePartition> stream();

    static LazyDegreePartitionIterator of(
        long nodeCount,
        long relationshipCount,
        int concurrency,
        PartitionUtils.DegreeFunction degrees
    ) {
        long numRelationshipsInPartition = BitUtil.ceilDiv(relationshipCount, concurrency * DIVISION_FACTOR);

        return new MultiDegreePartitionIterator(nodeCount, numRelationshipsInPartition, degrees);
    }

    private final static class MultiDegreePartitionIterator extends LazyDegreePartitionIterator {

        private final long nodeCount;
        private final PartitionUtils.DegreeFunction degrees;

        private long nextStartNode;
        private final long partitionSize;

        MultiDegreePartitionIterator(
            long nodeCount,
            long partitionSize,
            PartitionUtils.DegreeFunction degrees
        ) {
            this.nodeCount = nodeCount;
            this.degrees = degrees;
            this.nextStartNode = 0;
            this.partitionSize = partitionSize;
        }

        @Override
        protected DegreePartition fetch() {
            long nodeId = this.nextStartNode;

            if (nodeId >= nodeCount) {
                return done();
            }

            long relsInPartition = 0;
            long startNode = nodeId;

            for (; nodeId < nodeCount; nodeId++) {
                long nextRelationshipCount = degrees.degree(nodeId);

                relsInPartition += nextRelationshipCount;
                if (relsInPartition > partitionSize) {
                    this.nextStartNode = nodeId + 1;
                    return DegreePartition.of(
                        startNode,
                        nextStartNode - startNode,
                        relsInPartition
                    );
                }
            }

            this.nextStartNode = nodeId + 1;
            return DegreePartition.of(startNode, nodeCount - startNode, relsInPartition);
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
}
