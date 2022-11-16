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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSageTrainConfigTest {

    private static Stream<Arguments> invalidActivationFunctions() {
        return Stream.of(
            Arguments.of(1, "Expected ActivationFunction or String. Got Integer."),
            Arguments.of("alwaysTrue", "ActivationFunction `alwaysTrue` is not supported. Must be one of: ['RELU', 'SIGMOID'].")
        );
    }

    private static Stream<Arguments> invalidAggregator() {
        return Stream.of(
            Arguments.of(1, "Expected Aggregator or String. Got Integer."),
            Arguments.of("alwaysTrue", "Aggregator `alwaysTrue` is not supported. Must be one of: ['MEAN', 'POOL'].")
        );
    }

    @ParameterizedTest
    @CsvSource({
        "0.5, 100, 1",
        "0.2, 1000, 2",
        "0.99, 1000, 10",
    })
    void specifyBatchesPerIteration(double samplingRatio, long nodeCount, int expectedSampledBatches) {
        var mapWrapper = CypherMapWrapper.create(Map.of(
            "modelName", "foo",
            "featureProperties", List.of("a"),
            "batchSamplingRatio", samplingRatio,
            "batchSize", 100
        ));

        assertThat(GraphSageTrainConfig.of("user", mapWrapper).batchesPerIteration(nodeCount)).isEqualTo(expectedSampledBatches);
    }

    @Test
    void shouldThrowIfNoPropertiesProvided() {
        var mapWrapper = CypherMapWrapper.create(Map.of("modelName", "foo"));
        var expectedMessage = "No value specified for the mandatory configuration parameter `featureProperties`";
        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(expectedMessage);
    }

    @Test
    void shouldThrowIfEmptyPropertiesProvided() {
        var mapWrapper = CypherMapWrapper.create(Map.of("modelName", "foo", "featureProperties", List.of()));
        var expectedMessage = "GraphSage requires at least one property.";
        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(expectedMessage);
    }

    @ParameterizedTest
    @ValueSource(ints = {-20, 0})
    void failOnInvalidProjectedFeatureDimension(int projectedFeatureDimension) {
        var mapWrapper = CypherMapWrapper.create(Map.of(
            "modelName", "foo",
            "featureProperties", List.of("a"),
            "projectedFeatureDimension", projectedFeatureDimension
        ));

        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value for `projectedFeatureDimension` was `%d`", projectedFeatureDimension)
            .hasMessageContaining("must be within the range [1,");
    }

    @ParameterizedTest
    @ValueSource(ints = {-20, 0})
    void failOnInvalidEpochs(int projectedFeatureDimension) {
        var mapWrapper = CypherMapWrapper.create(Map.of(
            "modelName", "foo",
            "featureProperties", List.of("a"),
            "epochs", projectedFeatureDimension
        ));

        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value for `epochs` was `%d`", projectedFeatureDimension)
            .hasMessageContaining("must be within the range [1,");
    }

    @ParameterizedTest
    @MethodSource("invalidActivationFunctions")
    void failOnInvalidActivationFunction(Object activationFunction, String errorMessage) {
        var mapWrapper = CypherMapWrapper.create(Map.of("modelName", "foo", "activationFunction", activationFunction));
        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(errorMessage);
    }

    @ParameterizedTest
    @MethodSource("invalidAggregator")
    void failOnInvalidAggregator(Object aggregator, String errorMessage) {
        var mapWrapper = CypherMapWrapper.create(Map.of("modelName", "foo","aggregator", aggregator));
        assertThatThrownBy(() -> GraphSageTrainConfig.of("", mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(errorMessage);
    }

    @Test
    void shouldKnowIfMultiOrSingleLabel() {
        var multiLabelConfig = GraphSageTrainConfig.of(
            "",
            CypherMapWrapper.create(Map.of(
                "modelName", "graphSageModel",
                "projectedFeatureDimension", 42,
                "featureProperties", List.of("a")
            ))
        );
        assertTrue(multiLabelConfig.isMultiLabel());
        var singleLabelConfig = GraphSageTrainConfig.of(
            "",
            CypherMapWrapper.create(Map.of(
                "modelName", "graphSageModel",
                "featureProperties", List.of("a")
            ))
        );
        assertFalse(singleLabelConfig.isMultiLabel());
    }

    @Test
    void failOnLongSizedSamples() {
        assertThatThrownBy(() ->
            GraphSageTrainConfig.of(
                "",
                CypherMapWrapper.create(Map.of(
                    "modelName", "graphSageModel",
                    "featureProperties", List.of("one"),
                    "sampleSizes", List.of(Long.MAX_VALUE)
                ))
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasRootCauseExactlyInstanceOf(ArithmeticException.class)
            .hasMessageContaining("Sample size must smaller than 2^31");
    }

    @Test
    void failOnZeroSample() {
        assertThatThrownBy(() ->
            GraphSageTrainConfig.of(
                "",
                CypherMapWrapper.create(Map.of(
                    "modelName", "graphSageModel",
                    "featureProperties", List.of("one"),
                    "sampleSizes", List.of(1, 10, 0, 20)
                ))
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value for `sampleSizes` was `0`, but must be within the range [1,");
    }
}
