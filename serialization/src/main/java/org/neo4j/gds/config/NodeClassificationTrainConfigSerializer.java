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
package org.neo4j.gds.config;

import org.neo4j.gds.TrainConfigSerializer;
import org.neo4j.gds.ml.nodemodels.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.config.proto.CommonConfigProto;
import org.neo4j.gds.ml.model.proto.NodeClassificationProto;

import java.util.stream.Collectors;

import static org.neo4j.gds.config.ConfigSerializers.multiClassNLRTrainConfig;
import static org.neo4j.gds.config.ConfigSerializers.multiClassNLRTrainConfigMap;
import static org.neo4j.gds.config.ConfigSerializers.serializableFeaturePropertiesConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableModelConfig;

public final class NodeClassificationTrainConfigSerializer implements TrainConfigSerializer<NodeClassificationTrainConfig, NodeClassificationProto.NodeClassificationTrainConfig> {

    public NodeClassificationTrainConfigSerializer() {}

    public NodeClassificationProto.NodeClassificationTrainConfig toSerializable(NodeClassificationTrainConfig trainConfig) {
        var builder = NodeClassificationProto.NodeClassificationTrainConfig.newBuilder();

        builder
            .setModelConfig(serializableModelConfig(trainConfig))
            .setFeaturePropertiesConfig(serializableFeaturePropertiesConfig(trainConfig))
            .setHoldoutFraction(trainConfig.holdoutFraction())
            .setValidationFolds(trainConfig.validationFolds())
            .setTargetProperty(trainConfig.targetProperty());

        var randomSeedBuilder = CommonConfigProto.RandomSeed
            .newBuilder()
            .setPresent(trainConfig.randomSeed().isPresent());
        trainConfig.randomSeed().ifPresent(randomSeedBuilder::setValue);
        builder.setRandomSeed(randomSeedBuilder);

        trainConfig.metrics()
            .stream()
            .map(MetricSpecification::asString)
            .forEach(builder::addMetrics);

        trainConfig.paramsConfig().forEach(config -> {
            var trainingConfig = multiClassNLRTrainConfig(config);
            builder.addParamConfigs(trainingConfig);
        });

        return builder.build();
    }

    public NodeClassificationTrainConfig fromSerializable(NodeClassificationProto.NodeClassificationTrainConfig serializedTrainConfig) {
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

        serializedTrainConfig.getParamConfigsList().forEach(paramConfig -> {
            builder.addParam(multiClassNLRTrainConfigMap(paramConfig));
        });

        return builder.build();
    }

    @Override
    public Class<NodeClassificationProto.NodeClassificationTrainConfig> serializableClass() {
        return NodeClassificationProto.NodeClassificationTrainConfig.class;
    }
}
