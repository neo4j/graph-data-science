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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.subgraph.SubGraph;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class GraphSageEmbeddingsGenerator {
    private final Layer[] layers;
    private final int batchSize;
    private final int concurrency;
    private final FeatureFunction featureFunction;
    private final long randomSeed;
    private final ExecutorService executor;
    private final ProgressTracker progressTracker;

    public GraphSageEmbeddingsGenerator(
        Layer[] layers,
        int batchSize,
        int concurrency,
        FeatureFunction featureFunction,
        Optional<Long> randomSeed,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        this.layers = layers;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.featureFunction = featureFunction;
        this.randomSeed = randomSeed.orElseGet(() -> ThreadLocalRandom.current().nextLong());
        this.executor = executor;
        this.progressTracker = progressTracker;
    }

    public HugeObjectArray<double[]> makeEmbeddings(
        Graph graph,
        HugeObjectArray<double[]> features
    ) {
        HugeObjectArray<double[]> result = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount()
        );

        progressTracker.beginSubTask();

        var tasks = PartitionUtils.rangePartitionWithBatchSize(
            graph.nodeCount(),
            batchSize,
            partition -> createEmbeddings(graph.concurrentCopy(), partition, features, result)
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executor)
            .run();

        progressTracker.endSubTask();

        return result;
    }

    private Runnable createEmbeddings(
        Graph graph,
        Partition partition,
        HugeObjectArray<double[]> features,
        HugeObjectArray<double[]> result
    ) {
        return () -> {
            List<SubGraph> subGraphs = GraphSageHelper.subGraphsPerLayer(
                graph,
                partition.stream().toArray(),
                layers,
                randomSeed
            );

            Variable<Matrix> batchedFeaturesExtractor = featureFunction.apply(
                graph,
                subGraphs.get(subGraphs.size() - 1).originalNodeIds(),
                features
            );

            Variable<Matrix> embeddingVariable = GraphSageHelper.embeddingsComputationGraph(
                subGraphs,
                layers,
                batchedFeaturesExtractor
            );

            Matrix embeddings = new ComputationContext().forward(embeddingVariable);

            var partitionStartNodeId = partition.startNode();
            var partitionNodeCount = partition.nodeCount();
            for (int partitionIdx = 0; partitionIdx < partitionNodeCount; partitionIdx++) {
                long nodeId = partitionStartNodeId + partitionIdx;
                result.set(nodeId, embeddings.getRow(partitionIdx));
            }

            progressTracker.logProgress(partitionNodeCount);
        };
    }
}
