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

import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfigImpl;
import org.neo4j.graphalgo.config.proto.CommonConfigProto;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.ml.model.proto.CommonML;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public final class ConfigSerializers {

    private ConfigSerializers() {}

    static CommonConfigProto.ModelConfigProto.Builder serializableModelConfig(ModelConfig modelConfig) {
        return CommonConfigProto.ModelConfigProto
            .newBuilder()
            .setModelName(modelConfig.modelName());
    }

    static CommonConfigProto.EmbeddingDimensionConfigProto.Builder serializableEmbeddingDimensionsConfig(
        EmbeddingDimensionConfig embeddingDimensionConfig
    ) {
        return CommonConfigProto.EmbeddingDimensionConfigProto
            .newBuilder()
            .setEmbeddingDimension(embeddingDimensionConfig.embeddingDimension());
    }

    static CommonConfigProto.ToleranceConfigProto.Builder serializableToleranceConfig(ToleranceConfig toleranceConfig) {
        return CommonConfigProto.ToleranceConfigProto
            .newBuilder()
            .setTolerance(toleranceConfig.tolerance());
    }

    static CommonConfigProto.IterationsConfigProto.Builder serializableIterationsConfig(IterationsConfig iterationsConfig) {
        return CommonConfigProto.IterationsConfigProto
            .newBuilder()
            .setMaxIterations(iterationsConfig.maxIterations());
    }

    static CommonConfigProto.FeaturePropertiesConfigProto.Builder serializableFeaturePropertiesConfig(
        FeaturePropertiesConfig featurePropertiesConfig
    ) {
        return CommonConfigProto.FeaturePropertiesConfigProto
            .newBuilder()
            .addAllFeatureProperties(featurePropertiesConfig.featureProperties());
    }

    static CommonConfigProto.BatchSizeConfigProto.Builder serializableBatchSizeConfig(BatchSizeConfig batchSizeConfig) {
        return CommonConfigProto.BatchSizeConfigProto
            .newBuilder()
            .setBatchSize(batchSizeConfig.batchSize());
    }

    static void serializableRelationshipWeightConfig(
        RelationshipWeightConfig relationshipWeightConfig,
        Consumer<CommonConfigProto.RelationshipWeightConfigProto> relationshipWeightConfigProtoConsumer
    ) {
        Optional
            .ofNullable(relationshipWeightConfig.relationshipWeightProperty())
            .ifPresent(weightProperty -> relationshipWeightConfigProtoConsumer.accept(
                CommonConfigProto.RelationshipWeightConfigProto
                    .newBuilder()
                    .setRelationshipWeightProperty(weightProperty)
                    .build()
            ));

    }

    public static CommonML.MultiClassNLRTrainConfig.Builder multiClassNLRTrainConfig(
        NodeLogisticRegressionTrainConfig config
    ) {
        return CommonML.MultiClassNLRTrainConfig.newBuilder()
            .addAllFeatureProperties(config.featureProperties())
            .setTargetProperty(config.targetProperty())
            .setPenalty(config.penalty())
            .setTrainingConfig(serializableTrainingConfig(config));
    }

    public static NodeLogisticRegressionTrainConfig multiClassNLRTrainConfig(CommonML.MultiClassNLRTrainConfig protoConfig) {
        var rawParams = multiClassNLRTrainConfigMap(protoConfig);

        return new NodeLogisticRegressionTrainConfigImpl(
            protoConfig.getFeaturePropertiesList(),
            protoConfig.getTargetProperty(),
            CypherMapWrapper
                .create(rawParams)
        );
    }

    static Map<String, Object> multiClassNLRTrainConfigMap(CommonML.MultiClassNLRTrainConfig protoConfig) {
        var trainingConfig = protoConfig.getTrainingConfig();
        return Map.of(
            "penalty", protoConfig.getPenalty(),
            "batchSize", trainingConfig.getBatchSize(),
            "minEpochs", trainingConfig.getMinEpochs(),
            "maxEpochs", trainingConfig.getMaxEpochs(),
            "patience", trainingConfig.getPatience(),
            "tolerance", trainingConfig.getTolerance(),
            "sharedUpdater", trainingConfig.getSharedUpdater(),
            "concurrency", trainingConfig.getConcurrency()
        );
    }

    public static CommonML.LinkLogisticRegressionTrainConfig.Builder linkLogisticRegressionTrainConfig(
        LinkLogisticRegressionTrainConfig config
    ) {
        return CommonML.LinkLogisticRegressionTrainConfig.newBuilder()
            .addAllFeatureProperties(config.featureProperties())
            .setPenalty(config.penalty())
            .setLinkFeatureCombiner(config.linkFeatureCombiner())
            .setTrainingConfig(serializableTrainingConfig(config));
    }

    public static LinkLogisticRegressionTrainConfig linkLogisticRegressionTrainConfig(CommonML.LinkLogisticRegressionTrainConfig protoConfig) {
        var rawParams = linkLogisticRegressionTrainConfigMap(protoConfig);

        return new LinkLogisticRegressionTrainConfigImpl(
            protoConfig.getFeaturePropertiesList(),
            CypherMapWrapper.create(rawParams)
        );
    }

    static Map<String, Object> linkLogisticRegressionTrainConfigMap(CommonML.LinkLogisticRegressionTrainConfig protoConfig) {
        var trainingConfig = protoConfig.getTrainingConfig();
        return Map.of(
            "penalty", protoConfig.getPenalty(),
            "linkFeatureCombiner", protoConfig.getLinkFeatureCombiner(),
            "batchSize", trainingConfig.getBatchSize(),
            "minEpochs", trainingConfig.getMinEpochs(),
            "maxEpochs", trainingConfig.getMaxEpochs(),
            "patience", trainingConfig.getPatience(),
            "tolerance", trainingConfig.getTolerance(),
            "sharedUpdater", trainingConfig.getSharedUpdater(),
            "concurrency", trainingConfig.getConcurrency()
        );
    }

    private static ModelProto.TrainingConfig serializableTrainingConfig(TrainingConfig config) {
        return ModelProto.TrainingConfig.newBuilder()
            .setBatchSize(config.batchSize())
            .setMinEpochs(config.minEpochs())
            .setMaxEpochs(config.maxEpochs())
            .setPatience(config.patience())
            .setTolerance(config.tolerance())
            .setSharedUpdater(config.sharedUpdater())
            .setConcurrency(config.concurrency())
            .build();
    }
}
