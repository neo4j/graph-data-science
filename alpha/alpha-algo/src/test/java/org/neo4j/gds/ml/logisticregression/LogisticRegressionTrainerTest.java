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
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.Trainer;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LogisticRegressionTrainerTest {

    @Test
    void withBias() {
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
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.0306168925, 0.0306168891, 0.0306168857,
                0.0306168925, 0.0306168891, 0.0306168857,
                0.0315877663, 0.0315877652, 0.0315877641,
                -0.0312343505, -0.031234349, -0.031234349
            },
            Offset.offset(1e-9)
        );
    }

    @Test
    void concurrently() {
        HugeLongArray labels = HugeLongArray.newArray(20_000, AllocationTracker.empty());
        labels.setAll(i -> i%4);
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(labels),
            4,
            LinkLogisticRegressionTrainConfig.defaultConfig(),
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[20_000][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), HugeLongArray.of(2, 0, 0, 2, 7, -75));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.0921466861, 0.0921466834, 0.0921466806,
                0.0921466861, 0.0921466834, 0.0921466806,
                -0.0921466861, -0.092146683, -0.0921466806,
                -0.0921466861, -0.0921466834, -0.092146680
            },
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
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 5);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.039060120, 0.039060115, 0.039060111, 0.039060106, 0.039060101,
                0.039060120, 0.039060115, 0.039060111, 0.039060106, 0.039060101,
                0.041842096, 0.041842094, 0.041842093, 0.041842091, 0.041842090,
                -0.040879166, -0.040879165, -0.040879164, -0.040879163, -0.040879162
            },
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
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.0017972116, 0.0011842635, 7.8040244627E-4,
                0.0017972116, 0.0011842635, 7.8040244627E-4,
                0.0017972116, 0.0011842635, 7.8040244627E-4,
                -0.0019790847, -0.0019186863, -0.0017972116
            },
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
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                -0.0019999999, -0.0019414807, -0.0019414806,
                9.4736841915E-4, 4.4049618270E-4, 4.4049602108E-4,
                0.0019999999, 0.0018227464411596237, 0.0018227464,
                -9.4736841915E-4, -5.3836991394E-4, -5.3836986256E-4
            },
            Offset.offset(1e-9)
        );
    }


    public static final class TestFeatures implements Trainer.Features {

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
