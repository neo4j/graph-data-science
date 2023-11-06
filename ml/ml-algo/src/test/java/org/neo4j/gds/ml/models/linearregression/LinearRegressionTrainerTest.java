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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;

import java.util.List;

@ExtendWith(SoftAssertionsExtension.class)
class LinearRegressionTrainerTest {

    @Test
    void train(SoftAssertions softly) {
        LinearRegressionTrainer trainer = new LinearRegressionTrainer(
            4,
            LinearRegressionTrainConfig.DEFAULT,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        Features features = FeaturesFactory.wrap(List.of(
            new double[] {2, 4, 6},
            new double[] {1, 3, 5},
            new double[] {100, 2, 3}
        ));

        var targets = HugeDoubleArray.of(12, 9, 6);
        var trainSet = ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 2));

        var regressor = trainer.train(features, targets, trainSet);

        var expectedWeights = new Matrix(new double[]{0.05629, 0.09999, 0.09999}, 1, 3);
        softly.assertThat(regressor.data().weights().data()).matches(weights -> weights.equals(expectedWeights, 1e-5));
        softly.assertThat(regressor.data().bias().data()).matches(weights -> weights.equals(new Scalar(0.09999), 1e-5));
    }
}
