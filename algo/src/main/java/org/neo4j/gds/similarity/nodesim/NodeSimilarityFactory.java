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
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.similarity.SimilarityGraphBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

public class NodeSimilarityFactory<CONFIG extends NodeSimilarityBaseConfig> implements AlgorithmFactory<NodeSimilarity, CONFIG> {

    @Override
    public NodeSimilarity build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        var progressLogger = new BatchingProgressLogger(
            log,
            graph.relationshipCount(),
            "NodeSimilarity",
            configuration.concurrency(),
            eventTracker
        );

        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);

        return new NodeSimilarity(graph, configuration, Pools.DEFAULT, progressTracker, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        int topN = Math.abs(config.normalizedN());
        int topK = Math.abs(config.normalizedK());

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
            )
            .add("weights",
                MemoryEstimations.setup("", (dimensions, concurrency) -> {
                int averageDegree = dimensions.nodeCount() == 0
                    ? 0
                    : Math.toIntExact(dimensions.maxRelCount() / dimensions.nodeCount());
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
            builder.add("topN list", TopNList.memoryEstimation(topN));
        }
        return builder.build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            "compute",
            Tasks.leaf("prepare", graph.relationshipCount()),
            Tasks.leaf("compare")
        );
    }
}
