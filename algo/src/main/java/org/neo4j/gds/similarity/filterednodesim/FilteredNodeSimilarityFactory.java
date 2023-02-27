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
package org.neo4j.gds.similarity.filterednodesim;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.TopKMap;
import org.neo4j.gds.similarity.nodesim.TopNList;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

public class FilteredNodeSimilarityFactory<CONFIG extends FilteredNodeSimilarityBaseConfig> extends GraphAlgorithmFactory<NodeSimilarity, CONFIG> {

    @Override
    public String taskName() {
        return "FilteredNodeSimilarity";
    }

    @Override
    public NodeSimilarity build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var similarityComputer = configuration.similarityMetric().build(configuration.similarityCutoff());
        return new NodeSimilarity(
            graph,
            configuration,
            similarityComputer,
            configuration.sourceNodeFilter().toNodeFilter(graph),
            configuration.targetNodeFilter().toNodeFilter(graph),
            configuration.concurrency(),
            Pools.DEFAULT,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        int topN = Math.abs(config.normalizedN());
        int topK = Math.abs(config.normalizedK());

        MemoryEstimations.Builder builder = MemoryEstimations.builder(NodeSimilarity.class.getSimpleName())
            .perNode("node filter", nodeCount -> sizeOfLongArray(BitSet.bits2words(nodeCount)))
            .add(
                "vectors",
                MemoryEstimations.setup("", (dimensions, concurrency) -> {
                    int averageDegree = dimensions.nodeCount() == 0
                        ? 0
                        : Math.toIntExact(dimensions.relCountUpperBound() / dimensions.nodeCount());
                    long averageVectorSize = sizeOfLongArray(averageDegree);
                    return MemoryEstimations.builder(HugeObjectArray.class)
                        .perNode("array", nodeCount -> nodeCount * averageVectorSize).build();
                })
            )
            .add("weights",
                MemoryEstimations.setup("", (dimensions, concurrency) -> {
                    int averageDegree = dimensions.nodeCount() == 0
                        ? 0
                        : Math.toIntExact(dimensions.relCountUpperBound() / dimensions.nodeCount());
                    long averageVectorSize = sizeOfDoubleArray(averageDegree);
                    return MemoryEstimations.builder(HugeObjectArray.class)
                        .rangePerNode("array", nodeCount -> MemoryRange.of(0, nodeCount * averageVectorSize))
                        .build();
                }));
        if (config.computeToGraph() && !config.hasTopK()) {
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
            builder.add(
                "topN list",
                MemoryEstimations.setup("", (dimensions, concurrency) ->
                    TopNList.memoryEstimation(dimensions.nodeCount(), topN))
            );
        }
        return builder.build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("prepare", graph.relationshipCount()),
            Tasks.leaf("compare node pairs")
        );
    }
}
