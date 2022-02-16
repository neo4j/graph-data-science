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
package org.neo4j.gds.ml.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LogisticRegressionTrainerTest {

    @Test
    void test() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LinkLogisticRegressionTrainConfig.defaultConfig(),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{0.099999999, 0.098875829, 0.098875829, 0.098875829, 0.098875829},
            Offset.offset(1e-9)
        );
    }

    @Test
    void concurrently() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            4,
            LinkLogisticRegressionTrainConfig.defaultConfig(),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{0.099999999, 0.098875829, 0.098875829, 0.098875829, 0.098875829},
            Offset.offset(1e-9)
        );
    }

    @Test
    void withoutBias() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LinkLogisticRegressionTrainConfig.of(Map.of("useBiasFeature", false)),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isEmpty();
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{0.099999999, 0.0990137015, 0.099013701, 0.099013701, 0.099013701},
            Offset.offset(1e-9)
        );
    }

    @Test
    void usingPenaltyShouldGiveSmallerAbsoluteValueWeights() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 100, "maxEpochs", 100)),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i + 1) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty();
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{0.036291355, 0.018254952, 0.012209816},
            Offset.offset(1e-9)
        );
    }

    @Test
    void shouldHandleLargeValuedFeatures() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LinkLogisticRegressionTrainConfig.defaultConfig(),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                var factor = j == 0 ? 999_999_999 : 1;
                features[i][j] = Math.pow(-1, i) * factor * ((double) i + 1) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty();
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{0.001999999, 0.001979629, 0.001979629},
            Offset.offset(1e-9)
        );
    }


    private static final class TestFeatures implements Trainer.Features {

        private final double[][] features;

        TestFeatures(double[][] features) {
            this.features = features;
        }

        @Override
        public long size() {
            return features.length;
        }

        @Override
        public double[] get(long id) {
            return features[(int) id];
        }
    }

}
