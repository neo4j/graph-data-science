/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.BatchQueue;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.function.Consumer;

public class NodeClassificationPredictAlgorithm extends Algorithm<NodeClassificationPredictAlgorithm, MultiClassNLRResult> {

    private final MultiClassNLRPredictor predictor;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final boolean produceProbabilities;
    private final AllocationTracker tracker;

    NodeClassificationPredictAlgorithm(
        MultiClassNLRPredictor predictor,
        Graph graph,
        int batchSize,
        int concurrency,
        boolean produceProbabilities,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.predictor = predictor;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.produceProbabilities = produceProbabilities;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
    }

    @Override
    public MultiClassNLRResult compute() {
        progressLogger.logStart();
        var predictedProbabilities = initProbabilities();
        var predictedClasses = HugeLongArray.newArray(graph.nodeCount(), tracker);
        var consumer = new PredictConsumer(graph, predictor, predictedProbabilities, predictedClasses, progressLogger);
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize);
        batchQueue.parallelConsume(consumer, concurrency);
        progressLogger.logFinish();
        return MultiClassNLRResult.of(predictedClasses, predictedProbabilities);
    }

    @Override
    public NodeClassificationPredictAlgorithm me() {
        return this;
    }

    @Override
    public void release() {

    }

    private @Nullable HugeObjectArray<double[]> initProbabilities() {
        if (produceProbabilities) {
            var data = (MultiClassNLRData) predictor.modelData();
            var numberOfClasses = data.classIdMap().originalIds().length;
            var predictions = HugeObjectArray.newArray(
                double[].class,
                graph.nodeCount(),
                tracker
            );
            predictions.setAll(i -> new double[numberOfClasses]);
            return predictions;
        } else {
            return null;
        }
    }

    private static class PredictConsumer implements Consumer<Batch> {
        private final Graph graph;
        private final Predictor<Matrix, MultiClassNLRData> predictor;
        private final HugeObjectArray<double[]> predictedProbabilities;
        private final HugeLongArray predictedClasses;
        private final ProgressLogger progressLogger;

        PredictConsumer(
            Graph graph,
            Predictor<Matrix, MultiClassNLRData> predictor,
            @Nullable HugeObjectArray<double[]> predictedProbabilities,
            HugeLongArray predictedClasses,
            ProgressLogger progressLogger
        ) {
            this.graph = graph;
            this.predictor = predictor;
            this.predictedProbabilities = predictedProbabilities;
            this.predictedClasses = predictedClasses;
            this.progressLogger = progressLogger;
        }

        @Override
        public void accept(Batch batch) {
            var probabilityMatrix = predictor.predict(graph, batch);
            var numberOfClasses = probabilityMatrix.cols();
            var probabilities = probabilityMatrix.data();
            var currentRow = new MutableInt(0);
            batch.nodeIds().forEach(nodeId -> {
                var offset = currentRow.getAndIncrement() * numberOfClasses;
                if (predictedProbabilities != null) {
                    var probabilitiesForNode = new double[numberOfClasses];
                    System.arraycopy(probabilities, offset, probabilitiesForNode, 0, numberOfClasses);
                    predictedProbabilities.set(nodeId, probabilitiesForNode);
                }
                var bestClassId = -1;
                var maxProbability = -1d;
                for (int classId = 0; classId < numberOfClasses; classId++) {
                    var probability = probabilities[offset + classId];
                    if (probability > maxProbability) {
                        maxProbability = probability;
                        bestClassId = classId;
                    }
                }
                var bestClass = predictor.modelData().classIdMap().toOriginal(bestClassId);
                predictedClasses.set(nodeId, bestClass);
            });
            progressLogger.logProgress(batch.size());
        }
    }

}
