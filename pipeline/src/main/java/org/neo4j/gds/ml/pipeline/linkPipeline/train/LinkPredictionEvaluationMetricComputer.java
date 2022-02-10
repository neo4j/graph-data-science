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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class LinkPredictionEvaluationMetricComputer {

    private LinkPredictionEvaluationMetricComputer() {}

    static MemoryRange estimate(long relationshipSetSize) {
        return MemoryRange.of(SignedProbabilities.estimateMemory(relationshipSetSize));
    }

    static Map<LinkMetric, Double> computeMetric(
        FeaturesAndTargets inputData,
        LinkLogisticRegressionData modelData,
        BatchQueue evaluationQueue,
        LinkPredictionTrainConfig trainConfig,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        progressTracker.setVolume(inputData.size());

        var predictor = new LinkLogisticRegressionPredictor(modelData);
        var signedProbabilities = SignedProbabilities.create(inputData.size());
        var targets = inputData.targets();
        var features = inputData.features();

        evaluationQueue.parallelConsume(trainConfig.concurrency(), thread -> (batch) -> {
                for (Long relationshipIdx : batch.nodeIds()) {
                    double predictedProbability = predictor.predictedProbability(features.get(relationshipIdx));
                    boolean isEdge = targets.get(relationshipIdx) == 1.0D;

                    var signedProbability = isEdge ? predictedProbability : -1 * predictedProbability;
                    signedProbabilities.add(signedProbability);
                }

                progressTracker.logProgress(batch.size());
            },
            terminationFlag
        );

        return trainConfig.metrics().stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> metric.compute(signedProbabilities, trainConfig.negativeClassWeight())
        ));
    }
}
