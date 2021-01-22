/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

import java.util.Optional;

public final class GraphSageTrainConfigSerializer {

    private GraphSageTrainConfigSerializer() {}

    public static GraphSageProto.GraphSageTrainConfig toSerializable(GraphSageTrainConfig trainConfig) {
        var protoConfigBuilder = GraphSageProto.GraphSageTrainConfig.newBuilder();

        protoConfigBuilder
            .setModelConfig(
                CommonConfigProto.ModelConfigProto
                    .newBuilder()
                    .setModelName(trainConfig.modelName())
            )
            .setEmbeddingDimensionConfig(
                CommonConfigProto.EmbeddingDimensionConfigProto
                    .newBuilder()
                    .setEmbeddingDimension(trainConfig.embeddingDimension())
            )
            .addAllSampleSizes(trainConfig.sampleSizes())
            .setAggregator(GraphSageProto.AggregatorType.valueOf(trainConfig.aggregator().name()))
            .setActivationFunction(GraphSageProto.ActivationFunction.valueOf(trainConfig.activationFunction().name()))
            .setToleranceConfig(
                CommonConfigProto.ToleranceConfigProto
                    .newBuilder()
                    .setTolerance(trainConfig.tolerance())
            )
            .setLearningRate(trainConfig.learningRate())
            .setEpochs(trainConfig.epochs())
            .setIterationsConfig(
                CommonConfigProto.IterationsConfigProto
                    .newBuilder()
                    .setMaxIterations(trainConfig.maxIterations())
            )
            .setSearchDepth(trainConfig.searchDepth())
            .setNegativeSampleWeight(trainConfig.negativeSampleWeight())
            .setDegreeAsProperty(trainConfig.degreeAsProperty())
            .setFeaturePropertiesConfig(
                CommonConfigProto.FeaturePropertiesConfigProto
                    .newBuilder()
                    .addAllFeatureProperties(trainConfig.featureProperties())
            );

        var projectedFeatureDimensionBuilder = GraphSageProto.ProjectedFeatureDimension
            .newBuilder()
            .setPresent(trainConfig.projectedFeatureDimension().isPresent());
        trainConfig.projectedFeatureDimension().ifPresent(projectedFeatureDimensionBuilder::setValue);
        protoConfigBuilder.setProjectedFeatureDimension(projectedFeatureDimensionBuilder);

        Optional
            .ofNullable(trainConfig.relationshipWeightProperty())
            .ifPresent(weightProperty -> protoConfigBuilder.setRelationshipWeightConfig(
                CommonConfigProto.RelationshipWeightConfigProto
                    .newBuilder()
                    .setRelationshipWeightProperty(weightProperty)
            ));

        return protoConfigBuilder.build();
    }

    public static GraphSageTrainConfig fromSerializable(GraphSageProto.GraphSageTrainConfig protoTrainConfig) {
        var trainConfigBuilder = GraphSageTrainConfig.builder();

        trainConfigBuilder
            .modelName(protoTrainConfig.getModelConfig().getModelName())
            .aggregator(Aggregator.AggregatorType.of(protoTrainConfig.getAggregator().name()))
            .activationFunction(ActivationFunction.of(protoTrainConfig.getActivationFunction().name()))
            .featureProperties(protoTrainConfig.getFeaturePropertiesConfig().getFeaturePropertiesList())
            .degreeAsProperty(protoTrainConfig.getDegreeAsProperty());

        var projectedFeatureDimension = protoTrainConfig.getProjectedFeatureDimension();
        if (projectedFeatureDimension.getPresent()) {
            trainConfigBuilder.projectedFeatureDimension(projectedFeatureDimension.getValue());
        }

        var relationshipWeightPropertyCandidate = protoTrainConfig
            .getRelationshipWeightConfig()
            .getRelationshipWeightProperty();
        if (!relationshipWeightPropertyCandidate.isBlank()) {
            trainConfigBuilder.relationshipWeightProperty(relationshipWeightPropertyCandidate);
        }

        return trainConfigBuilder.build();
    }
}
