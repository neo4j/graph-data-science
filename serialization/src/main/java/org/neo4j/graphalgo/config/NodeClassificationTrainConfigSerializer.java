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

import org.neo4j.gds.TrainConfigSerializer;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.util.ObjectMapperSingleton;
import org.neo4j.graphalgo.core.model.proto.TrainConfigsProto;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.config.ConfigSerializers.serializableFeaturePropertiesConfig;
import static org.neo4j.graphalgo.config.ConfigSerializers.serializableModelConfig;

public final class NodeClassificationTrainConfigSerializer implements TrainConfigSerializer<NodeClassificationTrainConfig, TrainConfigsProto.NodeClassificationTrainConfig> {

    public NodeClassificationTrainConfigSerializer() {}

    public TrainConfigsProto.NodeClassificationTrainConfig toSerializable(NodeClassificationTrainConfig trainConfig) {
        var builder = TrainConfigsProto.NodeClassificationTrainConfig.newBuilder();

        builder
            .setModelConfig(serializableModelConfig(trainConfig))
            .setFeaturePropertiesConfig(serializableFeaturePropertiesConfig(trainConfig))
            .setHoldoutFraction(trainConfig.holdoutFraction())
            .setValidationFolds(trainConfig.validationFolds())
            .setTargetProperty(trainConfig.targetProperty());

        var randomSeedBuilder = TrainConfigsProto.RandomSeed
            .newBuilder()
            .setPresent(trainConfig.randomSeed().isPresent());
        trainConfig.randomSeed().ifPresent(randomSeedBuilder::setValue);
        builder.setRandomSeed(randomSeedBuilder);

        trainConfig.metrics()
            .stream()
            .map(MetricSpecification::asString)
            .forEach(builder::addMetrics);

        trainConfig.params().forEach(paramsMap -> {
            try {
                var p = ObjectMapperSingleton.OBJECT_MAPPER.writeValueAsString(paramsMap);
                builder.addParams(p);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return builder.build();
    }

    public NodeClassificationTrainConfig fromSerializable(TrainConfigsProto.NodeClassificationTrainConfig serializedTrainConfig) {
        var builder = NodeClassificationTrainConfig.builder();

        builder
            .modelName(serializedTrainConfig.getModelConfig().getModelName())
            .holdoutFraction(serializedTrainConfig.getHoldoutFraction())
            .validationFolds(serializedTrainConfig.getValidationFolds())
            .targetProperty(serializedTrainConfig.getTargetProperty())
            .featureProperties(serializedTrainConfig.getFeaturePropertiesConfig().getFeaturePropertiesList());

        var randomSeed = serializedTrainConfig.getRandomSeed();
        if (randomSeed.getPresent()) {
            builder.randomSeed(randomSeed.getValue());
        }

        var metrics = serializedTrainConfig
            .getMetricsList()
            .stream()
            .map(MetricSpecification::parse)
            .collect(Collectors.toList());
        builder.metrics(metrics);

        List<Map<String, Object>> params = serializedTrainConfig
            .getParamsList()
            .stream()
            .map(this::protoToMap)
            .collect(Collectors.toList());
        builder.params(params);

        return builder.build();
    }

    @Override
    public Class<TrainConfigsProto.NodeClassificationTrainConfig> serializableClass() {
        return TrainConfigsProto.NodeClassificationTrainConfig.class;
    }

    private Map<String, Object> protoToMap(String p) {
        try {
            return ObjectMapperSingleton.OBJECT_MAPPER.readValue(p, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
