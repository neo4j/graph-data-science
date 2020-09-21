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
package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.builder.GraphBuilder;
import org.neo4j.graphalgo.core.loading.builder.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;

public class TraversalToEdge extends Algorithm<TraversalToEdge, Relationships> {

    private final Graph[] graphs;
    private final long nodeCount;

    public TraversalToEdge(Graph[] graphs) {
        this.graphs = graphs;
        this.nodeCount = graphs[0].nodeCount();
    }

    @Override
    public Relationships compute() {
        var relImporter = GraphBuilder.createRelationshipsBuilder(
            ((HugeGraph)graphs[0]).idMap(),
            Orientation.NATURAL,
            false,
            Aggregation.NONE,
            false,
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        TraversalConsumer traversalConsumer = new TraversalConsumer(relImporter);
        long batchOffset = 0;
        long[] startNodes = new long[64];

        do {
            if (batchOffset + 64 >= nodeCount) {
                startNodes = new long[(int)(nodeCount - batchOffset)];
            }
            for (long j = batchOffset; j < Math.min(batchOffset + 64, nodeCount); j++) {
                startNodes[(int) (j - batchOffset)] = j;
            }


            MultiSourceBFS.traversalToEdge(
                graphs,
                traversalConsumer,
                AllocationTracker.empty(),
                startNodes
            ).run();

            batchOffset += 64;

        } while(batchOffset < nodeCount);

        return relImporter.build();
    }

    @Override
    public TraversalToEdge me() {
        return this;
    }

    @Override
    public void release() {

    }

    private static final class TraversalConsumer implements BfsConsumer {
        private final RelationshipsBuilder relImporter;

        private TraversalConsumer(RelationshipsBuilder relImporter) {this.relImporter = relImporter;}

        @Override
        public void accept(long targetNode, int depth, BfsSources sourceNode) {
            while (sourceNode.hasNext()) {
                relImporter.addFromInternal(sourceNode.next(), targetNode);
            }
        }
    }
}
