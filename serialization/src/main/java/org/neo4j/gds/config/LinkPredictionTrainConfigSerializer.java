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
import org.neo4j.gds.ml.linkmodels.LinkPredictionTrainConfig;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.config.proto.CommonConfigProto;
import org.neo4j.graphalgo.core.model.proto.TrainConfigsProto;

import static org.neo4j.gds.config.ConfigSerializers.linkLogisticRegressionTrainConfig;
import static org.neo4j.gds.config.ConfigSerializers.linkLogisticRegressionTrainConfigMap;
import static org.neo4j.gds.config.ConfigSerializers.serializableFeaturePropertiesConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableModelConfig;

public final class LinkPredictionTrainConfigSerializer implements TrainConfigSerializer<LinkPredictionTrainConfig, TrainConfigsProto.LinkPredictionTrainConfig> {

    public LinkPredictionTrainConfigSerializer() {}

    public TrainConfigsProto.LinkPredictionTrainConfig toSerializable(LinkPredictionTrainConfig trainConfig) {
        var builder = TrainConfigsProto.LinkPredictionTrainConfig.newBuilder();
        var randomSeedBuilder = CommonConfigProto.RandomSeed
            .newBuilder()
            .setPresent(trainConfig.randomSeed().isPresent());
        trainConfig.randomSeed().ifPresent(randomSeedBuilder::setValue);

        builder
            .setModelConfig(serializableModelConfig(trainConfig))
            .setFeaturePropertiesConfig(serializableFeaturePropertiesConfig(trainConfig))
            .setValidationFolds(trainConfig.validationFolds())
            .setNegativeClassWeight(trainConfig.negativeClassWeight())
            .setTrainRelationshipType(trainConfig.trainRelationshipType().name())
            .setTestRelationshipType(trainConfig.testRelationshipType().name())
            .setRandomSeed(randomSeedBuilder);

        trainConfig.paramConfigs().forEach(config -> {
            builder.addParamConfigs(linkLogisticRegressionTrainConfig(config));
        });

        return builder.build();
    }

    public LinkPredictionTrainConfig fromSerializable(TrainConfigsProto.LinkPredictionTrainConfig serializedTrainConfig) {
        var builder = LinkPredictionTrainConfig.builder();

        builder
            .modelName(serializedTrainConfig.getModelConfig().getModelName())
            .validationFolds(serializedTrainConfig.getValidationFolds())
            .negativeClassWeight(serializedTrainConfig.getNegativeClassWeight())
            .trainRelationshipType(RelationshipType.of(serializedTrainConfig.getTrainRelationshipType()))
            .testRelationshipType(RelationshipType.of(serializedTrainConfig.getTestRelationshipType()))
            .featureProperties(serializedTrainConfig.getFeaturePropertiesConfig().getFeaturePropertiesList());

        var randomSeed = serializedTrainConfig.getRandomSeed();
        if (randomSeed.getPresent()) {
            builder.randomSeed(randomSeed.getValue());
        }

        serializedTrainConfig.getParamConfigsList().forEach(paramConfig -> {
            builder.addParam(linkLogisticRegressionTrainConfigMap(paramConfig));
        });

        return builder.build();
    }

    @Override
    public Class<TrainConfigsProto.LinkPredictionTrainConfig> serializableClass() {
        return TrainConfigsProto.LinkPredictionTrainConfig.class;
    }
}
