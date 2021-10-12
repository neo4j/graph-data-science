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
package org.neo4j.gds.ml.nodemodels;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionResult;

import java.util.List;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.core.batch.BatchTransformer.IDENTITY;

public class NodeClassificationPredict extends Algorithm<NodeClassificationPredict, NodeLogisticRegressionResult> {

    static MemoryEstimation memoryEstimation(boolean produceProbabilities, int batchSize, int featureCount, int classCount) {
        var builder = MemoryEstimations.builder(NodeClassificationPredict.class);
        if (produceProbabilities) {
            builder.perNode("predicted probabilities", (nodeCount) -> HugeObjectArray.memoryEstimation(nodeCount, sizeOfDoubleArray(classCount)));
        }
        builder.perNode("predicted classes", HugeLongArray::memoryEstimation);
        builder.fixed("computation graph", NodeLogisticRegressionPredictor.sizeOfPredictionsVariableInBytes(batchSize, featureCount, classCount));
        return builder.build();
    }

    private final NodeLogisticRegressionPredictor predictor;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final boolean produceProbabilities;
    private final List<String> featureProperties;
    private final AllocationTracker allocationTracker;

    public NodeClassificationPredict(
        NodeLogisticRegressionPredictor predictor,
        Graph graph,
        int batchSize,
        int concurrency,
        boolean produceProbabilities,
        List<String> featureProperties,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.predictor = predictor;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.produceProbabilities = produceProbabilities;
        this.featureProperties = featureProperties;
        this.allocationTracker = allocationTracker;
    }

    @Override
    public NodeLogisticRegressionResult compute() {
        progressTracker.beginSubTask();
        var predictedProbabilities = initProbabilities();
        var predictedClasses = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            IDENTITY,
            predictor,
            predictedProbabilities,
            predictedClasses,
            featureProperties,
            progressTracker
        );
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize);
        batchQueue.parallelConsume(consumer, concurrency, terminationFlag);
        progressTracker.endSubTask();
        return NodeLogisticRegressionResult.of(predictedClasses, predictedProbabilities);
    }

    @Override
    public NodeClassificationPredict me() {
        return this;
    }

    @Override
    public void release() {

    }

    private @Nullable HugeObjectArray<double[]> initProbabilities() {
        if (produceProbabilities) {
            var data = (NodeLogisticRegressionData) predictor.modelData();
            var numberOfClasses = data.classIdMap().originalIds().length;
            var predictions = HugeObjectArray.newArray(
                double[].class,
                graph.nodeCount(),
                allocationTracker
            );
            predictions.setAll(i -> new double[numberOfClasses]);
            return predictions;
        } else {
            return null;
        }
    }
}
