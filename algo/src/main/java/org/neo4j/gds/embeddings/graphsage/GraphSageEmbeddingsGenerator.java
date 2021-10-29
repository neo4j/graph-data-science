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
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.embeddings;

public class GraphSageEmbeddingsGenerator {
    private final Layer[] layers;
    private final int batchSize;
    private final int concurrency;
    private final boolean isWeighted;
    private final FeatureFunction featureFunction;
    private final ExecutorService executor;
    private final ProgressTracker progressTracker;
    private final AllocationTracker allocationTracker;

    public GraphSageEmbeddingsGenerator(
        Layer[] layers,
        int batchSize,
        int concurrency,
        boolean isWeighted,
        FeatureFunction featureFunction,
        ExecutorService executor,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        this.layers = layers;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.isWeighted = isWeighted;
        this.featureFunction = featureFunction;
        this.executor = executor;
        this.progressTracker = progressTracker;
        this.allocationTracker = allocationTracker;
    }

    public HugeObjectArray<double[]> makeEmbeddings(
        Graph graph,
        HugeObjectArray<double[]> features
    ) {
        HugeObjectArray<double[]> result = HugeObjectArray.newArray(
            double[].class,
            graph.nodeCount(),
            allocationTracker
        );

        progressTracker.beginSubTask();

        var tasks = PartitionUtils.rangePartitionWithBatchSize(
            graph.nodeCount(),
            batchSize,
            partition -> createEmbeddings(graph, partition, features, result)
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

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
            ComputationContext ctx = new ComputationContext();
            Variable<Matrix> embeddingVariable = embeddings(
                graph,
                isWeighted,
                partition.stream().toArray(),
                features,
                layers,
                featureFunction
            );
            Matrix embeddings = ctx.forward(embeddingVariable);

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
