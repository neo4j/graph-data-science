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
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfigImpl;

import java.util.List;
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
    void shouldProduceToMapLRWithRanges() {
        var userInput = Map.of(
            "penalty", Map.of("range", List.of(0.1, 0.2)),
            "patience", Map.of("range", List.of(42, 1337)),
            "batchSize", 99
        );
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.LogisticRegression);
        assertThat(config.toMap()).isEqualTo(Map.of(
            "batchSize", 99,
            "learningRate", 0.001,
            "maxEpochs", 100,
            "methodName", "LogisticRegression",
            "minEpochs", 1,
            "patience", Map.of("range", List.of(42, 1337)),
            "penalty", Map.of("range", List.of(0.1, 0.2)),
            "tolerance", 0.001
        ));
    }

    @Test
    void shouldMaterializeLRConfig() {
        var userInput = Map.<String, Object>of("penalty", 0.1);
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.LogisticRegression);
        var trainerConfig = config.materialize(Map.of());
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(LogisticRegressionTrainConfigImpl.builder().penalty(0.1).build());
    }

    @Test
    void shouldMaterializeLRConfigWithRanges() {
        var userInput = Map.of(
            "penalty", Map.of("range", List.of(0.1, 0.2)),
            "patience", Map.of("range", List.of(42L, 1337L)),
            "batchSize", 99
        );
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.LogisticRegression);
        var trainerConfig = config.materialize(Map.of("penalty", 0.1337, "patience", 999));
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(LogisticRegressionTrainConfigImpl.builder().batchSize(99).penalty(0.1337).patience(999).build());
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
        var trainerConfig = config.materialize(Map.of());
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(RandomForestTrainConfigImpl.builder().maxDepth(5).build());
    }

    @Test
    void shouldMaterializeRFConfigWithRanges() {
        var userInput = Map.of(
            "maxDepth", 5L,
            "maxFeaturesRatio", Map.of("range", List.of(0.1, 0.2)),
            "numberOfDecisionTrees", Map.of("range", List.of(10, 100))
        );
        var config = TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest);
        var trainerConfig = config.materialize(Map.of(
            "maxFeaturesRatio", 0.1337,
            "numberOfDecisionTrees", 55
        ));
        assertThat(trainerConfig)
            .usingRecursiveComparison()
            .isEqualTo(RandomForestTrainConfigImpl
                .builder()
                .maxDepth(5)
                .maxFeaturesRatio(0.1337)
                .numberOfDecisionTrees(55)
                .build());
    }

    @Test
    void failsOnNonMapNonNumericValue() {
        var userInput = Map.<String, Object>of("maxDepth", "foo");
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Parameter `maxDepth` must be numeric or of the form {range: {min, max}}.")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failsOnIllegalMapKeys() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", "foo", "bar", "bat"));
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Map parameters must be of the form {range: {min, max}}, " +
                        "where both min and max are Float or Integer. Invalid keys: [`maxDepth`]")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failsOnIllegalMapValueType() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", "foo"));
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Map parameters must be of the form {range: {min, max}}, " +
                        "where both min and max are Float or Integer. Invalid keys: [`maxDepth`]")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failsOnIllegalRangeListSize() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", List.of(1,2,3)));
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Map parameters must be of the form {range: {min, max}}, " +
                        "where both min and max are Float or Integer. Invalid keys: [`maxDepth`]")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failsOnMixedRangeValueTypes() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", List.of(1,2L)));
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Map parameters must be of the form {range: {min, max}}, " +
                        "where both min and max are Float or Integer. Invalid keys: [`maxDepth`]")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failsOnIllegalRangeValueType() {
        var userInput = Map.<String, Object>of("maxDepth", Map.of("range", List.of("foo", "bar")));
        assertThatThrownBy(() -> TunableTrainerConfig.of(userInput, TrainingMethod.RandomForest))
            .hasMessage("Map parameters must be of the form {range: {min, max}}, " +
                        "where both min and max are Float or Integer. Invalid keys: [`maxDepth`]")
            .isInstanceOf(IllegalArgumentException.class);
    }

}
