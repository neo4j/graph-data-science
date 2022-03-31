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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.hyperparameter.HyperParameterValues;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfigImpl;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TunableTrainerConfigTest {
    @Test
    void shouldProduceToMapLR() {
        var userInput = Map.<String, Object>of("penalty", 0.1);
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.LogisticRegression);
        assertThat(config.toMap()).isEqualTo(Map.of(
            "batchSize", 100,
            "learningRate", 0.001,
            "maxEpochs", 100,
            "methodName", "LogisticRegression",
            "minEpochs", 1,
            "patience", 1,
            "penalty", 0.1,
            "tolerance", 0.001
        ));
    }

    @Test
    void shouldMaterializeLRConfig() {
        var userInput = Map.<String, Object>of("penalty", 0.1);
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.LogisticRegression);
        var trainerConfig = config.materialize(HyperParameterValues.EMPTY);
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(LogisticRegressionTrainConfigImpl.builder().penalty(0.1).build());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProduceToMapRF(boolean useLong) {
        var value = useLong ? 5L : 5;
        var userInput = Map.<String, Object>of("maxDepth", value);
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest);
        assertThat(config.toMap()).isEqualTo(Map.of(
            "maxDepth", 5,
            "methodName", "RandomForest",
            "minSplitSize", 2,
            "numberOfDecisionTrees", 100,
            "numberOfSamplesRatio", 1.0
        ));
    }

    @Test
    void shouldMaterializeRFConfig() {
        var userInput = Map.<String, Object>of("maxDepth", 5L);
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest);
        var trainerConfig = config.materialize(HyperParameterValues.EMPTY);
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(RandomForestTrainConfigImpl.builder().maxDepth(5).build());
    }

    @Test
    void failsOnIllegalParameterType() {
        var userInput = Map.<String, Object>of("maxDepth", "foo");
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Parameter `maxDepth` must be numeric")
            .isInstanceOf(IllegalArgumentException.class);
    }

}
