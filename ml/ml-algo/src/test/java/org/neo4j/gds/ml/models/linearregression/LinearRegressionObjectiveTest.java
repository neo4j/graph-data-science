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
package org.neo4j.gds.ml.models.linearregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.ListBatch;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LinearRegressionObjectiveTest {

    @Test
    void testLoss() {
        Features features = FeaturesFactory.wrap(
            // these have no effect because weights are zero
            Stream.of(-100.0, 420.1, 13.37).map(i -> new double[]{i}).collect(Collectors.toList())
        );

        var objective = new LinearRegressionObjective(
            features,
            HugeDoubleArray.of(1.0, 2.5, 3.5),
            0.0
        );

        Variable<Scalar> loss = objective.loss(new ListBatch(new long[] {0L, 1L, 2L}), 10);
        double lossValue = new ComputationContext().forward(loss).value();

        double expectedMeanSquareError = 6.5;
        assertThat(lossValue).isCloseTo(expectedMeanSquareError, Offset.offset(1e-10));

        var weights = objective.weights();

        assertThat(weights).hasSize(2);
        assertThat(weights.get(0).data()).isEqualTo(Matrix.create(0D, 1, features.featureDimension()));
        assertThat(weights.get(1).data()).isEqualTo(new Scalar(0D));
    }

    @Test
    void testLossWithBias() {
        var objective = new LinearRegressionObjective(
            FeaturesFactory.wrap(
                // these have no effect because weights are zero
                Stream.of(-100.0, 420.1, 13.37).map(i -> new double[]{i}).collect(Collectors.toList())
            ),
            HugeDoubleArray.of(1.0, 2.5, 3.5),
            0.0
        );
        // set the bias
        objective.weights().get(1).data().setDataAt(0, 3);

        Variable<Scalar> loss = objective.loss(new ListBatch(new long[] {0L, 1L, 2L}), 10);
        double lossValue = new ComputationContext().forward(loss).value();

        double expectedMeanSquareError = 1.5;
        assertThat(lossValue).isCloseTo(expectedMeanSquareError, Offset.offset(1e-10));
    }

    @Test
    void testLossWithWeightsAndBias() {
        var objective = new LinearRegressionObjective(
            FeaturesFactory.wrap(
                List.of(
                    new double[]{1.0, 2.0},
                    new double[]{-1.0, 4.2},
                    new double[]{7.5, -0.001}
                )
            ),
            HugeDoubleArray.of(1.0, 2.5, 3.5),
            0.0
        );
        // set the weights
        objective.weights().get(0).data().setDataAt(0, 1);
        objective.weights().get(0).data().setDataAt(1, 2);
        // set the bias
        objective.weights().get(1).data().setDataAt(0, 3);

        Variable<Scalar> loss = objective.loss(new ListBatch(new long[] {0L, 1L, 2L}), 10);
        double lossValue = new ComputationContext().forward(loss).value();

        double expectedMeanSquareError = 53.460668;
        assertThat(lossValue).isCloseTo(expectedMeanSquareError, Offset.offset(1e-10));
    }

    @ParameterizedTest
    @CsvSource(value = {
        "   1,   2,  5,      20.83666",
        "   1,   2, 10,      28.33666",
        "0.01, 100, 10,  115094.87057",
        "100,  110, 10,  235701.16666"
    }
    )
    void testLossWithWeightsAndPenalty(double firstWeight, double secondWeight, double penalty, double expectedLoss) {
        var objective = new LinearRegressionObjective(
            FeaturesFactory.wrap(
                List.of(
                    new double[]{1.0, 2.0},
                    new double[]{-1.0, 4.2},
                    new double[]{7.5, -2.0}
                )
            ),
            HugeDoubleArray.of(1.0, 2.5, 3.5),
            penalty
        );
        // set the weights
        objective.weights().get(0).data().setDataAt(0, firstWeight);
        objective.weights().get(0).data().setDataAt(1, secondWeight);

        Variable<Scalar> loss = objective.loss(new ListBatch(new long[] {0L, 1L, 2L}), 10);
        double lossValue = new ComputationContext().forward(loss).value();

        assertThat(lossValue).isCloseTo(expectedLoss, Offset.offset(1e-5));
    }

}
