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
package org.neo4j.gds.ml.nodeClassification;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetric;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.function.LongUnaryOperator;

import static org.neo4j.gds.ml.core.batch.BatchQueue.DEFAULT_BATCH_SIZE;

public final class ClassificationMetricComputer {

    private final HugeIntArray predictedClasses;
    private final HugeIntArray labels;

    private ClassificationMetricComputer(
        HugeIntArray predictedClasses,
        HugeIntArray labels
    ) {
        this.labels = labels;
        this.predictedClasses = predictedClasses;
    }

    public double score(ClassificationMetric metric) {
        return metric.compute(labels, predictedClasses);
    }

    public static ClassificationMetricComputer forEvaluationSet(
        Features features,
        HugeIntArray labels,
        ReadOnlyHugeLongArray evaluationSet,
        Classifier classifier,
        Concurrency concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        var predictor = new ParallelNodeClassifier(
            classifier,
            features,
            DEFAULT_BATCH_SIZE,
            concurrency,
            terminationFlag,
            progressTracker
        );

        return new ClassificationMetricComputer(
            predictor.predict(evaluationSet),
            makeLocalTargets(evaluationSet, labels)
        );
    }

    private static HugeIntArray makeLocalTargets(ReadOnlyHugeLongArray nodeIds, HugeIntArray targets) {
        var localTargets = HugeIntArray.newArray(nodeIds.size());

        localTargets.setAll(i -> targets.get(nodeIds.get(i)));
        return localTargets;
    }

    public static MemoryEstimation estimateEvaluation(
        TrainerConfig config,
        int batchSize,
        LongUnaryOperator trainSetSize,
        LongUnaryOperator testSetSize,
        int fudgedClassCount,
        int fudgedFeatureCount,
        boolean isReduced
    ) {
        return MemoryEstimations.builder("computing metrics")
            .perNode("local targets", nodeCount -> {
                var sizeOfLargePartOfAFold = testSetSize.applyAsLong(nodeCount);
                return HugeLongArray.memoryEstimation(sizeOfLargePartOfAFold);
            })
            .perNode("predicted classes", nodeCount -> {
                var sizeOfLargePartOfAFold = testSetSize.applyAsLong(nodeCount);
                return HugeLongArray.memoryEstimation(sizeOfLargePartOfAFold);
            })
            .add(
                "classifier model",
                ClassifierFactory.dataMemoryEstimation(
                    config,
                    trainSetSize,
                    fudgedClassCount,
                    fudgedFeatureCount,
                    isReduced
                )
            )
            .rangePerNode(
                "classifier runtime",
                nodeCount -> ClassifierFactory.runtimeOverheadMemoryEstimation(
                    config.method(),
                    batchSize,
                    fudgedClassCount,
                    fudgedFeatureCount,
                    isReduced
                )
            )
            .build();
    }

}
