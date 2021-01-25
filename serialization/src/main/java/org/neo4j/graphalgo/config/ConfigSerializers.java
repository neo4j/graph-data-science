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

import org.neo4j.graphalgo.config.proto.CommonConfigProto;

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
}
