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
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.splitting.EdgeSplitter;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class LinkPredictionEvaluationMetricComputer {

    private LinkPredictionEvaluationMetricComputer() {}

    static MemoryRange estimate(long relationshipSetSize) {
        return MemoryRange.of(SignedProbabilities.estimateMemory(relationshipSetSize));
    }

    static Map<LinkMetric, Double> computeMetric(
        FeaturesAndLabels inputData,
        Classifier classifier,
        BatchQueue evaluationQueue,
        LinkPredictionTrainConfig trainConfig,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        progressTracker.setVolume(inputData.size());

        var signedProbabilities = SignedProbabilities.create(inputData.size());
        var targets = inputData.labels();
        var features = inputData.features();

        evaluationQueue.parallelConsume(trainConfig.concurrency(), thread -> (batch) -> {
                for (Long relationshipIdx : batch.nodeIds()) {
                    double[] classProbabilities = classifier.predictProbabilities(relationshipIdx, features);
                    int positiveClassId = classifier.classIdMap().toMapped((long) EdgeSplitter.POSITIVE);
                    double probabilityOfPositiveEdge = classProbabilities[positiveClassId];
                    boolean isEdge = targets.get(relationshipIdx) == EdgeSplitter.POSITIVE;

                    var signedProbability = isEdge ? probabilityOfPositiveEdge : -1 * probabilityOfPositiveEdge;
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
