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
package org.neo4j.gds.ml.nodePropertyPrediction.regression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NodeRegressionPredictTest {

    @Test
    void predict() {
        var features = FeaturesFactory.wrap(List.of(
            new double[] {1, 2, 3},
            new double[] {-1, 2, -1},
            new double[] {1, 2, 1}
        ));

        var regressor = new TestRegressor(3);

        NodeRegressionPredict algo = new NodeRegressionPredict(
            regressor,
            features,
            1,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        assertThat(algo.compute().toArray()).containsExactly(6D, 0D, 4D);
    }

    private static class TestRegressor implements Regressor {

        private final int featureDimensions;

        TestRegressor(int expectedFeatureDimension) {

            this.featureDimensions = expectedFeatureDimension;
        }

        @Override
        public double predict(double[] features) {
            return Arrays.stream(features).sum();
        }

        @Override
        public RegressorData data() {
            return new RegressorData() {
                @Override
                public TrainingMethod trainerMethod() {
                    return TrainingMethod.LinearRegression;
                }

                @Override
                public int featureDimension() {
                    return featureDimensions;
                }
            };
        }
    }
}
