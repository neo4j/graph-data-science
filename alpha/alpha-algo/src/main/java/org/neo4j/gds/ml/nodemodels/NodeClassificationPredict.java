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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.models.FeaturesFactory;
import org.neo4j.gds.models.logisticregression.LogisticRegressionClassifier;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.core.batch.BatchTransformer.IDENTITY;

public class NodeClassificationPredict extends Algorithm<NodeClassificationPredict.NodeClassificationResult> {

    private final LogisticRegressionClassifier classifier;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final boolean produceProbabilities;
    private final List<String> featureProperties;

    public NodeClassificationPredict(
        LogisticRegressionClassifier classifier,
        Graph graph,
        int batchSize,
        int concurrency,
        boolean produceProbabilities,
        List<String> featureProperties,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.classifier = classifier;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.produceProbabilities = produceProbabilities;
        this.featureProperties = featureProperties;
    }

    public static MemoryEstimation memoryEstimation(boolean produceProbabilities, int batchSize, int featureCount, int classCount) {
        var builder = MemoryEstimations.builder(NodeClassificationPredict.class);
        if (produceProbabilities) {
            builder.perNode("predicted probabilities", nodeCount -> HugeObjectArray.memoryEstimation(nodeCount, sizeOfDoubleArray(classCount)));
        }
        builder.perNode("predicted classes", HugeLongArray::memoryEstimation);
        builder.fixed("computation graph", LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(batchSize, featureCount, classCount));
        return builder.build();
    }

    public static MemoryEstimation memoryEstimationWithDerivedBatchSize(
        boolean produceProbabilities,
        int minBatchSize,
        int featureCount,
        int classCount
    ) {
        var builder = MemoryEstimations.builder(NodeClassificationPredict.class);
        if (produceProbabilities) {
            builder.perNode(
                "predicted probabilities",
                nodeCount -> HugeObjectArray.memoryEstimation(nodeCount, sizeOfDoubleArray(classCount))
            );
        }
        builder.perNode("predicted classes", HugeLongArray::memoryEstimation);
        builder.perGraphDimension(
            "computation graph",
            (dim, threads) -> MemoryRange.of(LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(BatchQueue.computeBatchSize(
                dim.nodeCount(),
                minBatchSize,
                threads
            ), featureCount, classCount))
        );
        return builder.build();
    }

    @Override
    public NodeClassificationResult compute() {
        progressTracker.beginSubTask();
        var features = FeaturesFactory.extractLazyFeatures(graph, featureProperties);
        var predictedProbabilities = initProbabilities();
        var predictedClasses = HugeLongArray.newArray(graph.nodeCount());
        var consumer = new NodeClassificationPredictConsumer(
            features,
            IDENTITY,
            classifier,
            predictedProbabilities,
            predictedClasses,
            progressTracker
        );
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize, concurrency);
        batchQueue.parallelConsume(consumer, concurrency, terminationFlag);
        progressTracker.endSubTask();
        return NodeClassificationResult.of(predictedClasses, predictedProbabilities);
    }

    @Override
    public void release() {

    }

    private @Nullable HugeObjectArray<double[]> initProbabilities() {
        if (produceProbabilities) {
            var numberOfClasses = classifier.numberOfClasses();
            var predictions = HugeObjectArray.newArray(
                double[].class,
                graph.nodeCount()
            );
            predictions.setAll(i -> new double[numberOfClasses]);
            return predictions;
        } else {
            return null;
        }
    }

    @ValueClass
    public interface NodeClassificationResult {

        HugeLongArray predictedClasses();
        Optional<HugeObjectArray<double[]>> predictedProbabilities();

        static NodeClassificationResult of(
            HugeLongArray classes,
            @Nullable HugeObjectArray<double[]> probabilities
        ) {
            return ImmutableNodeClassificationResult.builder()
                .predictedProbabilities(Optional.ofNullable(probabilities))
                .predictedClasses(classes)
                .build();
        }
    }
}
