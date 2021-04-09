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
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRPredictor;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRResult;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.List;

import static org.neo4j.gds.ml.batch.BatchTransformer.IDENTITY;

public class NodeClassificationPredict extends Algorithm<NodeClassificationPredict, MultiClassNLRResult> {

    private final MultiClassNLRPredictor predictor;
    private final Graph graph;
    private final int batchSize;
    private final int concurrency;
    private final boolean produceProbabilities;
    private final List<String> featureProperties;
    private final AllocationTracker tracker;

    NodeClassificationPredict(
        MultiClassNLRPredictor predictor,
        Graph graph,
        int batchSize,
        int concurrency,
        boolean produceProbabilities,
        List<String> featureProperties,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.predictor = predictor;
        this.graph = graph;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.produceProbabilities = produceProbabilities;
        this.featureProperties = featureProperties;
        this.tracker = tracker;
        this.progressLogger = progressLogger;
    }

    @Override
    public MultiClassNLRResult compute() {
        progressLogger.logStart();
        var predictedProbabilities = initProbabilities();
        var predictedClasses = HugeLongArray.newArray(graph.nodeCount(), tracker);
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            IDENTITY,
            predictor,
            predictedProbabilities,
            predictedClasses,
            featureProperties,
            progressLogger
        );
        var batchQueue = new BatchQueue(graph.nodeCount(), batchSize);
        batchQueue.parallelConsume(consumer, concurrency);
        progressLogger.logFinish();
        return MultiClassNLRResult.of(predictedClasses, predictedProbabilities);
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
}
