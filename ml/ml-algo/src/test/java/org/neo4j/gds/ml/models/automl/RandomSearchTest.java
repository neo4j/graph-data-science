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
package org.neo4j.gds.ml.models.automl;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfig;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RandomSearchTest {
    @Test
    void shouldSampleMethodsRoughlyEqually() {
        var maxTrials = 10000;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.LogisticRegression,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("penalty", Map.of("range", List.of(1e-4, 1e4))),
                        TrainingMethod.LogisticRegression
                    )
                ),
                TrainingMethod.RandomForestClassification,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "maxFeaturesRatio", Map.of("range", List.of(0.0, 1.0))
                        ),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    )
                )
            ),
            maxTrials,
            // passed 2500 times
            System.currentTimeMillis()
        );
        var randomForestSamples = 0;
        while (randomSearch.hasNext()) {
            var trainerConfig = randomSearch.next();
            if (trainerConfig instanceof RandomForestTrainerConfig) randomForestSamples++;
        }
        assertThat(randomForestSamples).isCloseTo((int) (0.5 * maxTrials), Offset.offset((int) (0.05 * maxTrials)));
    }

    @Test
    void shouldProduceLogScaleSamples() {
        var maxTrials = 10000;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.LogisticRegression,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "penalty", Map.of("range", List.of(1e-4, 1e4))
                        ),
                        TrainingMethod.LogisticRegression
                    )
                )
            ),
            maxTrials,
            // passed 2500 times
            System.currentTimeMillis()
        );
        // in log scale, the midpoint is 1.0
        var penaltiesHigherThanOne = 0;
        while (randomSearch.hasNext()) {
            var trainerConfig = randomSearch.next();
            assertThat(trainerConfig).isInstanceOf(LogisticRegressionTrainConfig.class);
            var lrConfig = (LogisticRegressionTrainConfig) trainerConfig;
            assertThat(lrConfig.penalty()).isBetween(1e-4, 1e4);
            if (lrConfig.penalty() > 1.0) penaltiesHigherThanOne++;
        }
        assertThat(penaltiesHigherThanOne).isCloseTo((int) (0.5 * maxTrials), Offset.offset((int) (0.05 * maxTrials)));
    }

    @Test
    void shouldProduceLinearScaleSamples() {
        var maxTrials = 10000;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.RandomForestClassification,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "maxFeaturesRatio", Map.of("range", List.of(0.0, 1.0))
                        ),
                        TrainingMethod.RandomForestClassification
                    )
                )
            ),
            maxTrials,
            // passed 2500 times
            System.currentTimeMillis()
        );
        var penaltiesHigherThanAHalf = 0;
        while (randomSearch.hasNext()) {
            var trainerConfig = randomSearch.next();
            assertThat(trainerConfig).isInstanceOf(RandomForestTrainerConfig.class);
            var lrConfig = (RandomForestTrainerConfig) trainerConfig;
            var maxFeaturesRatio = lrConfig.maxFeaturesRatio();
            assertThat(maxFeaturesRatio).isPresent().get(InstanceOfAssertFactories.DOUBLE).isBetween(0.0, 1.0);
            if (maxFeaturesRatio.get() > 0.5) penaltiesHigherThanAHalf++;
        }
        assertThat(penaltiesHigherThanAHalf).isCloseTo((int) (0.5 * maxTrials), Offset.offset((int) (0.05 * maxTrials)));
    }

    @Test
    void shouldProduceConcreteConfigsFirst() {
        var maxTrials = 1;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.LogisticRegression,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "penalty", Map.of("range", List.of(1e-4, 1e4))
                        ),
                        TrainingMethod.LogisticRegression
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    )
                )
            ),
            maxTrials,
            System.currentTimeMillis()
        );
        assertThat(randomSearch.hasNext()).isTrue();
        assertThat(randomSearch.next()).isInstanceOf(RandomForestTrainerConfig.class);
        assertThat(randomSearch.hasNext()).isTrue();
        assertThat(randomSearch.next()).isInstanceOf(LogisticRegressionTrainConfig.class);
        assertThat(randomSearch.hasNext()).isFalse();
    }

    @Test
    void runsAllConcreteConfigsRegardlessOfMaxTrials() {
        var maxTrials = 2;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.LogisticRegression,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "penalty", Map.of("range", List.of(1e-4, 1e4))
                        ),
                        TrainingMethod.LogisticRegression
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    )
                )
            ),
            maxTrials,
            System.currentTimeMillis()
        );
        // first all the concrete configs
        for (int i = 0; i < 4; i++) {
            assertThat(randomSearch.hasNext()).isTrue();
            assertThat(randomSearch.next()).isInstanceOf(RandomForestTrainerConfig.class);
        }
        // then maxTrials (2) auto tuning configs
        assertThat(randomSearch.hasNext()).isTrue();
        assertThat(randomSearch.next()).isInstanceOf(LogisticRegressionTrainConfig.class);
        assertThat(randomSearch.hasNext()).isTrue();
        assertThat(randomSearch.next()).isInstanceOf(LogisticRegressionTrainConfig.class);
        // then no more
        assertThat(randomSearch.hasNext()).isFalse();
    }

    @Test
    void foo() {
        var maxTrials = 5;
        var randomSearch = new RandomSearch(
            Map.of(
                TrainingMethod.LogisticRegression,
                List.of(
                    TunableTrainerConfig.of(
                        Map.of(
                            "penalty", Map.of("range", List.of(1e-4, 1e4))
                        ),
                        TrainingMethod.LogisticRegression
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    ),
                    TunableTrainerConfig.of(
                        Map.of(),
                        TrainingMethod.RandomForestClassification
                    )
                )
            ),
            maxTrials,
            System.currentTimeMillis()
        );
        // first all the concrete configs
        for (int i = 0; i < 8; i++) {
            assertThat(randomSearch.hasNext()).isTrue();
            randomSearch.next();
        }
        // then no more
        assertThat(randomSearch.hasNext()).isFalse();
    }
}
