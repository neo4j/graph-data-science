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
package org.neo4j.gds.models.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.batch.LazyBatch;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ReducedSoftmax;
import org.neo4j.gds.ml.core.functions.Softmax;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Tensor;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.models.FeaturesFactory;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static org.assertj.core.api.Assertions.assertThat;

class LogisticRegressionObjectiveTest {
    private HugeLongArray labels;

    private static Stream<Arguments> featureBatches() {
        return Stream.of(
            Arguments.of(new LazyBatch(0, 2, 10), new Matrix(new double[]{0, 0, 1, 1}, 2, 2)),
            Arguments.of(new LazyBatch(4, 3, 10), new Matrix(new double[]{4, 4, 5, 5, 6, 6}, 3, 2))
        );
    }

    private LogisticRegressionObjective standardObjective;
    private LogisticRegressionObjective reducedObjective;
    private LogisticRegressionObjective trainedStandardObjective;
    private LogisticRegressionObjective trainedReducedObjective;

    @BeforeEach
    void setup() {
        var featuresHOA = HugeObjectArray.of(
            new double[]{Math.pow(0.7, 2), Math.pow(0.7, 2)},
            new double[]{Math.pow(-1, 2), Math.pow(1.7, 2)},
            new double[]{Math.pow(1, 2), Math.pow(-1.6, 2)},
            new double[]{Math.pow(0.3, 2), Math.pow(-0.4, 2)}
        );

        labels = HugeLongArray.newArray(featuresHOA.size());
        labels.setAll(idx -> (idx < 2) ? 1 : 0);
        var idMap = new LocalIdMap();
        for (long i = 0; i < labels.size(); i++) {
            idMap.toMapped(labels.get(i));
        }

        var standardClassifier = new LogisticRegressionClassifier(
            LogisticRegressionData.standard(2, true, idMap)
        );
        var reducedClassifier = new LogisticRegressionClassifier(
            LogisticRegressionData.withReducedClassCount(2, true, idMap)
        );
        var trainedStandardClassifier = new LogisticRegressionClassifier(
            LogisticRegressionData.standard(2, true, idMap)
        );
        Arrays.setAll(trainedStandardClassifier.data().weights().data().data(), i -> i);
        Arrays.setAll(trainedStandardClassifier.data().bias().orElseThrow().data().data(), i -> i == 0 ? 0.4 : 0.8);
        var trainedReducedClassifier = new LogisticRegressionClassifier(
            LogisticRegressionData.withReducedClassCount(2, true, idMap)
        );
        Arrays.setAll(trainedReducedClassifier.data().weights().data().data(), i -> i);
        Arrays.setAll(trainedReducedClassifier.data().bias().orElseThrow().data().data(), i -> i == 0 ? 0.4 : 0.8);
        var features = FeaturesFactory.wrap(featuresHOA);
        this.standardObjective = new LogisticRegressionObjective(
            standardClassifier,
            1.0,
            features,
            labels
        );
        this.reducedObjective = new LogisticRegressionObjective(
            reducedClassifier,
            1.0,
            features,
            labels
        );
        this.trainedStandardObjective = new LogisticRegressionObjective(
            trainedStandardClassifier,
            1.0,
            features,
            labels
        );
        this.trainedReducedObjective = new LogisticRegressionObjective(
            trainedReducedClassifier,
            1.0,
            features,
            labels
        );
    }

    @Test
    void makeTargets() {
        var batch = new LazyBatch(1, 2, 4);
        var batchedTargets = standardObjective.batchLabelVector(batch, standardObjective.modelData().classIdMap());

        // original labels are 1.0, 0.0 , but these are local ids. since nodeId 0 has label 1.0, this maps to 0.0.
        assertThat(batchedTargets.data()).isEqualTo(new Vector(0.0, 1.0));
    }

    @ParameterizedTest
    @MethodSource("featureBatches")
    void shouldComputeCorrectFeatures(Batch batch, Tensor<?> expected) {
        var featureCount = 2;

        var allFeatures = HugeObjectArray.newArray(double[].class, 10);

        allFeatures.setAll(idx -> {
            double[] features = new double[featureCount];
            Arrays.fill(features, idx);
            return features;
        });

        Constant<Matrix> batchFeatures = LogisticRegressionClassifier.batchFeatureMatrix(batch,
            FeaturesFactory.wrap(allFeatures));

        assertThat(batchFeatures.data()).isEqualTo(expected);
    }

    @Test
    void lossStandard() {
        var batch = new LazyBatch(0, 4, 4);
        var loss = standardObjective.loss(batch, 4);

        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero. penalty part of objective is 0. remaining part of CEL is -Math.log(0.5).
        assertThat(lossValue).isEqualTo(-Math.log(0.5), Offset.offset(1E-9));
    }

    @Test
    void lossReduced() {
        var batch = new LazyBatch(0, 4, 4);
        var loss = reducedObjective.loss(batch, 4);

        var ctx = new ComputationContext();
        var lossValue = ctx.forward(loss).value();

        // weights are zero. penalty part of objective is 0. remaining part of CEL is -Math.log(0.5).
        assertThat(lossValue).isEqualTo(-Math.log(0.5), Offset.offset(1E-9));
    }

    @Test
    void standardObjective() {
        testLoss(Softmax.class, standardObjective);
    }
    @Test
    void reducedObjective() {
        testLoss(ReducedSoftmax.class, reducedObjective);
    }
    @Test
    void trainedStandardObjective() {
        testLoss(Softmax.class, trainedStandardObjective);
    }
    @Test
    void trainedReducedObjective() {
        testLoss(ReducedSoftmax.class, trainedReducedObjective);
    }

    <T extends Variable<Matrix>> void testLoss(Class softmaxClass, LogisticRegressionObjective objective) {
        var trainSize = 42;
        var ctx = new ComputationContext();
        var batch = new LazyBatch(0, 4, 4);
        var loss = objective.loss(batch, trainSize);
        var lossValue = ctx.forward(loss).value();

        var expectedPenalty = 0.0;
        for (var weight : objective.modelData().weights().data().data()) {
            expectedPenalty += weight * weight * batch.size() / trainSize;
        }
        var actualPenalty = ctx.forward(objective.penaltyForBatch(batch, trainSize)).value();
        assertThat(actualPenalty).isEqualTo(expectedPenalty, Offset.offset(1e-9));

        var predictions = (T) ctx
            .computedVariables()
            .stream()
            .filter(v -> v.getClass() == softmaxClass)
            .findFirst()
            .get();
        Matrix predictedValues = ctx.data(predictions);
        var idMap = objective.modelData().classIdMap();
        var expectedUnpenalizedLoss = 1.0/4.0 * (
            -Math.log(predictedValues.dataAt(0, idMap.toMapped((int)labels.get(0))))
            -Math.log(predictedValues.dataAt(1, idMap.toMapped((int)labels.get(1))))
            -Math.log(predictedValues.dataAt(2, idMap.toMapped((int)labels.get(2))))
            -Math.log(predictedValues.dataAt(3, idMap.toMapped((int)labels.get(3))))
        );
        var actualUnpenalizedLoss = ctx.forward(objective.crossEntropyLoss(batch)).value();
        assertThat(actualUnpenalizedLoss).isEqualTo(expectedUnpenalizedLoss, Offset.offset(1e-9));

        assertThat(lossValue).isEqualTo(expectedUnpenalizedLoss + expectedPenalty, Offset.offset(1e-9));
    }

    @ParameterizedTest
    @CsvSource(value = {
        " 10,   1, false,  808",
        " 10,   1, true, 1_000",
        "100,   1, true, 6_760",
        " 10, 100, true, 9_712",
    })
    void shouldEstimateMemoryUsage(int batchSize, int featureDim, boolean useBias, long expected) {
        var memoryUsageInBytes = LogisticRegressionObjective.sizeOfBatchInBytes(batchSize, featureDim, useBias);
        assertThat(memoryUsageInBytes).isEqualTo(expected);
    }

    @Test
    void shouldEstimateMemoryUsage() {
        var memoryUsageInBytes = LogisticRegressionObjective.sizeOfBatchInBytes(100, 10, 10);

        var weightGradient = 8 * 10 * 10 + 16;      // 8 bytes for a double * numberOfClasses * numberOfFeatures + 16 for the double array
        var makeTargets = 8 * 100 * 1 + 16;         // 8 bytes for a double * batchSize * 1 for the single target property + 16 for the double array
        var weightedFeatures = 8 * 100 * 10 + 16;   // 8 bytes for a double * batchSize * numberOfClasses + 16 for the double array
        var softMax = 8 * 100 * 10 + 16;            // 8 bytes for a double * batchSize * numberOfClasses + 16 for the double array
        var unpenalizedLoss = 24;                   // 8 bytes for a double + 16 for the double array
        var l2norm = 8 + 16;                        // 8 bytes for a double + 16 for the double array
        var constantScale = 8 + 16;                 // 8 bytes for a double + 16 for the double array
        var elementSum = 8 + 16;                    // 8 bytes for a double + 16 for the double array
        var predictor = 24368;                      // black box, not from this class

        var trainEpoch = makeTargets +
                         weightedFeatures +
                         softMax +
                         2 * unpenalizedLoss +
                         2 * l2norm +
                         2 * constantScale +
                         2 * elementSum +
                         predictor;

        var evaluateLoss = makeTargets +
                           weightedFeatures +
                           softMax +
                           unpenalizedLoss +
                           l2norm +
                           constantScale +
                           elementSum +
                           predictor;

        var expected = max(trainEpoch + weightGradient, evaluateLoss);
        assertThat(makeTargets).isEqualTo(Matrix.sizeInBytes(100, 1));
        assertThat(memoryUsageInBytes).isEqualTo(expected);
    }
}
