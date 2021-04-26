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
package org.neo4j.graphalgo.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.nodemodels.ImmutableNodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.util.ObjectMapperSingleton;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.gds.ml.nodemodels.metrics.MetricSpecificationTest.allValidMetricSpecifications;

class NodeClassificationTrainConfigSerializerTest {

    @ParameterizedTest
    @MethodSource("allValidMetricSpecificationsProxy")
    void shouldSerializeConfig(String metric) {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxEpochs", 1);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxEpochs", 10000, "tolerance", 1e-5);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(List.of(MetricSpecification.parse(metric)))
            .params(List.of(model1, model2))
            .build();

        var trainConfigSerializer = new NodeClassificationTrainConfigSerializer();
        var serializedConfig = trainConfigSerializer.toSerializable(config);

        assertThat(serializedConfig).isNotNull();

        assertThat(serializedConfig.getModelConfig().getModelName()).isEqualTo(config.modelName());
        assertThat(serializedConfig.getFeaturePropertiesConfig().getFeaturePropertiesList()).containsExactly("a", "b");

        assertThat(serializedConfig.getHoldoutFraction()).isEqualTo(config.holdoutFraction());
        assertThat(serializedConfig.getValidationFolds()).isEqualTo(config.validationFolds());
        assertThat(serializedConfig.getTargetProperty()).isEqualTo("t");
        assertThat(serializedConfig.getRandomSeed().getValue()).isEqualTo(19L);
        assertThat(serializedConfig.getMetricsList()).containsExactly(metric);

        assertThat(serializedConfig.getParamsList()).hasSize(2);

        var collect = serializedConfig.getParamsList()
            .stream()
            .map(this::protoToMap)
            .collect(Collectors.toList());
        assertThat(collect).containsExactly(model1, model2);
    }

    @Test
    void shouldSerializeConfigWithMultipleMetrics() {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxEpochs", 1);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(MetricSpecification.parse(allValidMetricSpecifications()))
            .params(List.of(model1))
            .build();

        var trainConfigSerializer = new NodeClassificationTrainConfigSerializer();
        var serializedConfig = trainConfigSerializer.toSerializable(config);

        assertThat(serializedConfig).isNotNull();

        assertThat(serializedConfig.getMetricsList()).isEqualTo(allValidMetricSpecifications());
    }

    @Test
    void shouldDeserializeConfig() {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxEpochs", 1);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxEpochs", 10000, "tolerance", 1e-5);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(MetricSpecification.parse(allValidMetricSpecifications()))
            .params(List.of(model1, model2))
            .build();

        var trainConfigSerializer = new NodeClassificationTrainConfigSerializer();
        var serializedConfig = trainConfigSerializer.toSerializable(config);

        var deserializedConfig = trainConfigSerializer.fromSerializable(serializedConfig);

        assertThat(deserializedConfig).isNotNull();

        assertThat(deserializedConfig.modelName()).isEqualTo("model");
        assertThat(deserializedConfig.featureProperties()).containsExactly("a", "b");
        assertThat(deserializedConfig.holdoutFraction()).isEqualTo(0.33);
        assertThat(deserializedConfig.validationFolds()).isEqualTo(2);
        assertThat(deserializedConfig.randomSeed()).isPresent().hasValue(19L);
        assertThat(deserializedConfig.targetProperty()).isEqualTo("t");
        assertThat(deserializedConfig.metrics()).isEqualTo(MetricSpecification.parse(allValidMetricSpecifications()));

        assertThat(deserializedConfig.params()).containsExactly(model1, model2);
    }

    @Test
    void shouldValidateParamsMap() {
        Map<String, Object> model1 = Map.of("penlty", 1, "maxEpochs", 1);

        assertThatThrownBy(() ->
            ImmutableNodeClassificationTrainConfig.builder()
                .modelName("model")
                .featureProperties(List.of("a", "b"))
                .holdoutFraction(0.33)
                .validationFolds(2)
                .concurrency(1)
                .randomSeed(19L)
                .targetProperty("t")
                .metrics(List.of(MetricSpecification.parse("F1_WEIGHTED")))
                .params(List.of(model1))
                .build())
            .hasMessageContaining("No value specified for the mandatory configuration parameter `penalty` (a similar parameter exists: [penlty])");
    }

    private Map<String, Object> protoToMap(String p) {
        try {
            return ObjectMapperSingleton.OBJECT_MAPPER.readValue(p, Map.class);
        } catch (Exception e) {
            fail(e.getMessage());
        }
        return Collections.emptyMap();
    }

    static List<String> allValidMetricSpecificationsProxy() {
        return allValidMetricSpecifications();
    }


}
