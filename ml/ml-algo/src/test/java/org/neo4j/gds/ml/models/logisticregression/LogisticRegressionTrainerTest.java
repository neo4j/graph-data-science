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
package org.neo4j.gds.ml.models.logisticregression;

import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestFeatures;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class LogisticRegressionTrainerTest {

    private static final HugeIntArray FOUR_CLASSES = HugeIntArray.of(0, 1, 1, 0, 2, 3);

    @Test
    void withBias() {
        var trainer = new LogisticRegressionTrainer(
            1,
            LogisticRegressionTrainConfig.DEFAULT,
            4,
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(
            new TestFeatures(features),
            FOUR_CLASSES,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(0) != 0D);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(1) != 0D);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(2) != 0D);
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
            4,
            LogisticRegressionTrainConfig.DEFAULT,
            4,
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        double[][] features = new double[20_000][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i) / (j + 1);
            }
        }
        var classifier = trainer.train(new TestFeatures(features), FOUR_CLASSES, ReadOnlyHugeLongArray.of(labels));

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(0) != 0D);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(1) != 0D);
        assertThat(classifier.data().bias()).matches(w -> w.data().dataAt(2) != 0D);
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
    void usingStandardWeights() {
        var trainer = new LogisticRegressionTrainer(
            1,
            LogisticRegressionTrainConfig.DEFAULT,
            4,
            false,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        var random = new Random(42L);
        double[][] features = new double[5][5];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = random.nextDouble();
            }
        }
        var classifier = trainer.train(
            new TestFeatures(features),
            FOUR_CLASSES,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
        assertThat(classifier.data().weights().data().dimensions()).containsExactly(4, 5);
        assertThat(classifier.data().weights().data().data())
            .containsExactly(
                new double[]{
                    0.08627900105266102, 0.09273267314678312, 0.09349891894008305, -0.11487502366240651, 0.09650781245420756,
                    0.09289478267235998, 0.08184372875603671, 0.08582250030378356, 0.08989331517422305, 0.09095783501089064,
                    -0.08027455601660115, 0.13006700122492873, -0.060498675944529436, 0.10485891962810245, -0.09612502145924187,
                    -0.0930896892159113, -0.0930382197013168, -0.09321662310635541, -0.09305099223387062, -0.09312873059916749
                },
                Offset.offset(1e-9)
            );
        assertThat(classifier.data().bias().data().data())
            .containsExactly(
                new double[]{
                    0.0928284537874426, 0.08881935818609046, -0.07553836988915141, -0.09312217411510736
                },
                Offset.offset(1e-9)
            );
    }

    @Test
    void usingPenaltyShouldGiveSmallerAbsoluteValueWeights() {
        var trainer = new LogisticRegressionTrainer(
            1,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 100, "maxEpochs", 100)),
            4,
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                features[i][j] = ((double) i + 1) / (j + 1);
            }
        }
        var classifier = trainer.train(
            new TestFeatures(features),
            FOUR_CLASSES,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
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
            1,
            LogisticRegressionTrainConfig.DEFAULT,
            4,
            true,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );

        double[][] features = new double[5][3];
        for (int i = 0; i < features.length; i++) {
            for (int j = 0; j < features[i].length; j++) {
                var factor = j == 0 ? 999_999_999 : 1;
                features[i][j] = Math.pow(-1, i) * factor * ((double) i + 1) / (j + 1);
            }
        }
        var classifier = trainer.train(
            new TestFeatures(features),
            FOUR_CLASSES,
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 1, 2, 3, 4))
        );

        assertThat(classifier.numberOfClasses()).isEqualTo(4);
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
        // same training
        var classifier1 = trainer().train(
            TestFeatures.singleConstant(1.0),
            HugeIntArray.of(0),
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0))
        );
        var classifier2 = trainer().train(
            TestFeatures.singleConstant(1.0),
            HugeIntArray.of(0),
            ReadOnlyHugeLongArray.of(HugeLongArray.of(0, 0, 0))
        );

        // same weights; penalty is scaled accordingly
        assertThat(classifier1.data().weights().data().data()).containsExactly(classifier2
            .data()
            .weights()
            .data()
            .data(), Offset.offset(1e-10));
    }

    @NotNull
    private LogisticRegressionTrainer trainer() {
        return new LogisticRegressionTrainer(
            1,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 1L)),
            1,
            false,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER,
            LogLevel.INFO
        );
    }


}
