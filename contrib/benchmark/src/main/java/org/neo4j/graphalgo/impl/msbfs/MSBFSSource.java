/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

public enum MSBFSSource {

    _1024_64(1024, 64),
    _1024_128(1024, 256),
    _1024_1024(1024),

    _4096_64(4096, 64),
    _4096_128(4096, 256),
    _4096_1024(4096, 1024),
    _4096_4096(4096),

    _16384_64(16384, 64),
    _16384_128(16384, 256),
    _16384_1024(16384, 1024),
    _16384_4096(16384, 4096),
    _16384_16384(16384);

    final DirectIdMapping nodes;
    final RelationshipIterator rels;
    final RelationshipIterator hugeRels;
    final int[] sources;
    final long[] hugeSources;

    MSBFSSource(int nodeCount, int sourceCount) {
        this.nodes = new DirectIdMapping(nodeCount);
        this.rels = new AllNodes(nodeCount);
        this.hugeRels = new HugeAllNodes(nodeCount);
        this.sources = new int[sourceCount];
        this.hugeSources = new long[sourceCount];
        Arrays.setAll(sources, i -> i);
        Arrays.setAll(hugeSources, i -> i);
    }

    MSBFSSource(int nodeCount) {
        this.nodes = new DirectIdMapping(nodeCount);
        this.rels = new AllNodes(nodeCount);
        this.hugeRels = new HugeAllNodes(nodeCount);
        this.sources = null;
        this.hugeSources = null;
    }

    private static final class AllNodes implements RelationshipIterator {

        private final int nodeCount;

        private AllNodes(final int nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
            for (int i = 0; i < nodeCount; i++) {
                if (i != nodeId) {
                    consumer.accept(nodeId, i);
                }
            }
        }

        // TODO: Not sure what to put here.
        @Override
        public void forEachRelationship(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        }
    }

    private static final class HugeAllNodes implements RelationshipIterator {

        private final long nodeCount;

        private HugeAllNodes(final long nodeCount) {
            this.nodeCount = nodeCount;
        }

        @Override
        public void forEachRelationship(
                long nodeId,
                Direction direction,
                RelationshipConsumer consumer) {
            for (long i = 0; i < nodeCount; i++) {
                if (i != nodeId) {
                    consumer.accept(nodeId, i);
                }
            }
        }

        @Override
        public void forEachRelationship(
                long nodeId,
                Direction direction,
                WeightedRelationshipConsumer consumer) {
            forEachRelationship(nodeId, direction, (s, t) -> consumer.accept(s, t, Double.NaN));
        }
    }
}
