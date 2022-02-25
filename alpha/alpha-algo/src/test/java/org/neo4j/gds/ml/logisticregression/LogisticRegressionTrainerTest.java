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
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LogisticRegressionTrainerTest {

    private static final HugeLongArray FOUR_CLASSES = HugeLongArray.of(2, 0, 0, 2, 7, -75);

    private static LocalIdMap fourClassIdMap() {
        var idMap = new LocalIdMap();
        for (long i = 0; i < FOUR_CLASSES.size(); i++) {
            idMap.toMapped(FOUR_CLASSES.get(i));
        }
        return idMap;
    }

    @Test
    void withBias() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LogisticRegressionTrainConfig.defaultConfig(),
            fourClassIdMap(),
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.00199977923, 0.00199977903, 0.00199977883,
                0.00199977923, 0.00199977903, 0.00199977883,
                0.00199992845, 0.00199992838, 0.00199992831
            },
            Offset.offset(1e-11)
        );
    }

    @Test
    void concurrently() {
        HugeLongArray labels = HugeLongArray.newArray(20_000);
        labels.setAll(i -> i % 4);
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(labels),
            4,
            LogisticRegressionTrainConfig.defaultConfig(),
            fourClassIdMap(),
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[20_000][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().value() != 0D);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.0938383655, 0.0938383627, 0.0938383600,
                0.0938383655, 0.0938383627, 0.0938383600,
                -0.091255531, -0.0912555285, -0.0912555257
            },
            Offset.offset(1e-9)
        );
    }

    @Test
    void withoutBias() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LogisticRegressionTrainConfig.of(Map.of("useBiasFeature", false)),
            fourClassIdMap(),
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isEmpty();
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 5);
        assertThat(classifier.data().weights().data().data())
            .containsExactly(
                new double[]{
                    0.001999766, 0.001999766, 0.001999766, 0.001999766, 0.001999766,
                    0.001999766, 0.001999766, 0.001999766, 0.001999766, 0.001999766,
                    0.001999924, 0.001999924, 0.001999924, 0.001999924, 0.001999924
                },
                Offset.offset(1e-9)
            );
    }

    @Test
    void usingStandardWeights() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LogisticRegressionTrainConfig.of(Map.of("useBiasFeature", false)),
            fourClassIdMap(),
            false,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isEmpty();
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 5);
        assertThat(classifier.data().weights().data().data())
            .containsExactly(
                new double[]{
                    0.059906731262955704, 0.05990672786615244, 0.05990672446934952, 0.05990672107254702, 0.05990671767574492,
                    0.059906731262955704, 0.059906727866152434, 0.05990672446934951, 0.05990672107254702, 0.05990671767574492,
                    0.06257710759125375, 0.06257710592417495, 0.06257710425709619, 0.06257710259001756, 0.06257710092293897,
                    -0.05930114848368565, -0.05930114677572412, -0.05930114506776269, -0.05930114335980138, -0.059301141651840146
                },
                Offset.offset(1e-9)
            );
    }

    @Test
    void usingPenaltyShouldGiveSmallerAbsoluteValueWeights() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 100, "maxEpochs", 100)),
            fourClassIdMap(),
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i + 1) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty();
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.001799698, 0.001187329, 0.000781799,
                0.001799698, 0.001187329, 0.000781799,
                0.001799698, 0.001187329, 0.000781799
            },
            Offset.offset(1e-9)
        );
    }

    @Test
    void shouldHandleLargeValuedFeatures() {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4)),
            1,
            LogisticRegressionTrainConfig.defaultConfig(),
            fourClassIdMap(),
            true,
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
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isNotEmpty();
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                -0.000947368419, -0.001058125018, -0.001058125004,
                0.000947368419, 0.000305467914, 0.000305467728,
                0.001999999995, 0.001752452950, 0.001752452926
            },
            Offset.offset(1e-12)
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
