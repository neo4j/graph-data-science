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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.ml.nodemodels.logisticregression.ImmutableNodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.util.ObjectMapperSingleton;
import org.neo4j.graphalgo.core.model.proto.TrainConfigsProto;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationTrainConfigSerializerTest {

    @ParameterizedTest
    @EnumSource(Metric.class)
    void shouldSerializeConfig(Metric metric) {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxIterations", 0);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxIterations", 10000, "tolerance", 1e-5);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(List.of(metric))
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
        assertThat(serializedConfig.getMetricsList()).containsExactly(TrainConfigsProto.Metric.valueOf(metric.name()));

        assertThat(serializedConfig.getParamsList()).hasSize(2);

        var collect = serializedConfig.getParamsList()
            .stream()
            .map(this::protoToMap)
            .collect(Collectors.toList());
        assertThat(collect).containsExactly(model1, model2);
    }

    @Test
    void shouldSerializeConfigWithMultipleMetrics() {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxIterations", 0);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(List.of(Metric.ACCURACY, Metric.F1_MACRO, Metric.F1_WEIGHTED))
            .params(List.of(model1))
            .build();

        var trainConfigSerializer = new NodeClassificationTrainConfigSerializer();
        var serializedConfig = trainConfigSerializer.toSerializable(config);

        assertThat(serializedConfig).isNotNull();

        assertThat(serializedConfig.getMetricsList()).containsExactly(
            TrainConfigsProto.Metric.ACCURACY,
            TrainConfigsProto.Metric.F1_MACRO,
            TrainConfigsProto.Metric.F1_WEIGHTED
        );
    }

    @Test
    void shouldDeserializeConfig() {
        Map<String, Object> model1 = Map.of("penalty", 1, "maxIterations", 0);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxIterations", 10000, "tolerance", 1e-5);

        var config = ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(List.of("a", "b"))
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(List.of(Metric.ACCURACY, Metric.F1_MACRO, Metric.F1_WEIGHTED))
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
        assertThat(deserializedConfig.metrics()).containsExactly(
            Metric.ACCURACY,
            Metric.F1_MACRO,
            Metric.F1_WEIGHTED
        );

        assertThat(deserializedConfig.params()).containsExactly(model1, model2);
    }

    private Map<String, Object> protoToMap(String p) {
        try {
            return ObjectMapperSingleton.OBJECT_MAPPER.readValue(p, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return Collections.emptyMap();
    }

}
