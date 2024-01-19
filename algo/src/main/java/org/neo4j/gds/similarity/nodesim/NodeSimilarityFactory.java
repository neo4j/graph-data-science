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
package org.neo4j.gds.similarity.nodesim;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccMemoryEstimateDefinition;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

public class NodeSimilarityFactory<CONFIG extends NodeSimilarityBaseConfig> extends GraphAlgorithmFactory<NodeSimilarity, CONFIG> {

    @Override
    public String taskName() {
        return "NodeSimilarity";
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
            configuration.concurrency(),
            DefaultPool.INSTANCE,
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
        if (config.enableComponentsOptimization()) {
            builder.perNode("nodes sorted by component", HugeLongArray::memoryEstimation);
            builder.perNode("upper bound per component", HugeAtomicLongArray::memoryEstimation);
            
            if (config.actuallyRunWCC()) {
                builder.add("wcc", new WccMemoryEstimateDefinition().memoryEstimation(false));
            } else {
                builder.perNode("component mapping", HugeLongArray::memoryEstimation);
            }
        }
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
            prepareTask(graph, config.actuallyRunWCC()),
            Tasks.leaf("compare node pairs")
        );
    }

    private Task prepareTask(Graph graph, boolean runWcc) {
        if (runWcc) {
            return Tasks.task(
                "prepare",
                new WccAlgorithmFactory<>().progressTask(graph),
                Tasks.leaf("initialize", graph.relationshipCount())
            );
        }
        return Tasks.leaf("prepare", graph.relationshipCount());
    }

}
