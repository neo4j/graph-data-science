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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestFeatures;
import org.neo4j.gds.TestLocalIdMap;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Map;
import java.util.Random;

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
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(0) != 0D);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(1) != 0D);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(2) != 0D);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.001999707328540424, 0.0019997071280373367, 0.0019997069275342902,
                0.001999707328540424, 0.0019997071280373367, 0.0019997069275342902,
                0.0019999946590813496, 0.0019999945924114946, 0.0019999945257416444
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
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(0) != 0D);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(1) != 0D);
        assertThat(classifier.data().bias()).isNotEmpty().get().matches(w -> w.data().dataAt(2) != 0D);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(3, 3);
        assertThat(classifier.data().weights().data().data()).containsExactly(
            new double[]{
                0.093050465, 0.093050463, 0.093050460,
                0.093050465, 0.093050463, 0.093050460,
                -0.088693323, -0.088693320, -0.088693318
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

        var random = new Random(42L);
        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = random.nextDouble();
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES);

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).isEmpty();
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 5);
        assertThat(classifier.data().weights().data().data())
            .containsExactly(
                new double[]{
                    0.07112165177005575, 0.07282159911961641, 0.07299361494706944, -0.08118839640670544, 0.0738451516049731,
                    0.07250321634094545, 0.06926188286299505, 0.07036007869555168, 0.07155818423783798, 0.071881927159593,
                    -0.070415705597294, 0.09351752899399764, -0.06700072894326915, 0.07646075088422129, -0.07382078874569262,
                    -0.07247742154547929, -0.07244736978411997, -0.07254489591102062, -0.07245099306947704, -0.07250321395784994

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
                0.001798823419447968, 0.0011862464619943674, 7.813062407504739E-4,
                0.0017988234194479679, 0.0011862464619943674, 7.813062407504741E-4,
                0.0018023058593831845, 0.0011905760984642403, 7.832819611133551E-4
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

    @Test
    void shouldApplyPenaltyScaledWithTrainSetSize() {
        // different train set sizes
        var trainer1 = trainer(ReadOnlyHugeLongArray.of(HugeLongArray.of(0)));
        var trainer2 = trainer(ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 0, 0)));

        // same training
        var classifier1 = trainer1.train(TestFeatures.singleConstant(1.0), HugeLongArray.of(0));
        var classifier2 = trainer2.train(TestFeatures.singleConstant(1.0), HugeLongArray.of(0));

        // same weights; penalty is scaled accordingly
        assertThat(classifier1.data().weights().data().data()).containsExactly(classifier2
            .data()
            .weights()
            .data()
            .data(), Offset.offset(1e-10));
    }

    @NotNull
    private LogisticRegressionTrainer trainer(ReadOnlyHugeLongArray trainSetLarge) {
        return new LogisticRegressionTrainer(
            trainSetLarge,
            1,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 1L)),
            TestLocalIdMap.identityMapOf(0),
            false,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );
    }


}
