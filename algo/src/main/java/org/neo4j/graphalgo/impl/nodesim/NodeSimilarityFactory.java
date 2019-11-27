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

package org.neo4j.graphalgo.impl.nodesim;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

public class NodeSimilarityFactory extends AlgorithmFactory<NodeSimilarity> {

    private final NodeSimilarity.Config config;
    private final boolean computesSimilarityGraph;
    public NodeSimilarityFactory(NodeSimilarity.Config config, boolean computesSimilarityGraph) {
        this.config = config;
        this.computesSimilarityGraph = computesSimilarityGraph;
    }

    @Override
    public NodeSimilarity build(Graph graph, ProcedureConfiguration configuration, AllocationTracker tracker, Log log) {
        return new NodeSimilarity(graph, config, Pools.DEFAULT, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        int topN = Math.abs(config.topN());
        int topK = Math.abs(config.topK());

        MemoryEstimations.Builder builder = MemoryEstimations.builder(NodeSimilarity.class)
            .perNode("node filter", nodeCount -> sizeOfLongArray(BitSet.bits2words(nodeCount)))
            .add(
                "vectors",
                MemoryEstimations.setup("", (dimensions, concurrency) -> {
                    int averageDegree = dimensions.nodeCount() == 0
                        ? 0
                        : Math.toIntExact(dimensions.maxRelCount() / dimensions.nodeCount());
                    long averageVectorSize = sizeOfLongArray(averageDegree);
                    return MemoryEstimations.builder(HugeObjectArray.class)
                        .perNode("array", nodeCount -> nodeCount * averageVectorSize).build();
                })
            );
        if (computesSimilarityGraph && !config.hasTopK()) {
            builder.add(
                "similarity graph",
                SimilarityGraphBuilder.memoryEstimation(topK, topN)
            );
        }
        if (config.hasTopK()) {
            builder.add(
                "topK map",
                MemoryEstimations.setup("", (dimensions, concurrency) ->
                    TopKMap.memoryEstimation(dimensions.nodeCount(), topK))
            );
        }
        if (config.hasTopN()) {
            builder.add("topN list", TopNList.memoryEstimation(topN));
        }
        return builder.build();
    }
}
