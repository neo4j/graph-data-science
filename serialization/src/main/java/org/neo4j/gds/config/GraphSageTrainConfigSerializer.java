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
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.config.proto.CommonConfigProto;
import org.neo4j.gds.core.model.proto.GraphSageCommonProto;
import org.neo4j.gds.core.model.proto.TrainConfigsProto;

import static org.neo4j.gds.config.ConfigSerializers.serializableBatchSizeConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableEmbeddingDimensionsConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableFeaturePropertiesConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableIterationsConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableModelConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableRelationshipWeightConfig;
import static org.neo4j.gds.config.ConfigSerializers.serializableToleranceConfig;

public final class GraphSageTrainConfigSerializer implements TrainConfigSerializer<GraphSageTrainConfig, TrainConfigsProto.GraphSageTrainConfig> {

    public GraphSageTrainConfigSerializer() {}

    @Override
    public TrainConfigsProto.GraphSageTrainConfig toSerializable(GraphSageTrainConfig trainConfig) {
        var protoConfigBuilder = TrainConfigsProto.GraphSageTrainConfig.newBuilder();

        var randomSeedBuilder = CommonConfigProto.RandomSeed
            .newBuilder()
            .setPresent(trainConfig.randomSeed().isPresent());
        trainConfig.randomSeed().ifPresent(randomSeedBuilder::setValue);

        protoConfigBuilder
            .setModelConfig(serializableModelConfig(trainConfig))
            .setEmbeddingDimensionConfig(serializableEmbeddingDimensionsConfig(trainConfig))
            .setAggregator(GraphSageCommonProto.AggregatorType.valueOf(trainConfig.aggregator().name()))
            .setActivationFunction(GraphSageCommonProto.ActivationFunction.valueOf(trainConfig
                .activationFunction()
                .name()))
            .addAllSampleSizes(trainConfig.sampleSizes())
            .setBatchSizeConfig(serializableBatchSizeConfig(trainConfig))
            .setToleranceConfig(serializableToleranceConfig(trainConfig))
            .setLearningRate(trainConfig.learningRate())
            .setEpochs(trainConfig.epochs())
            .setIterationsConfig(serializableIterationsConfig(trainConfig))
            .setSearchDepth(trainConfig.searchDepth())
            .setRandomSeed(randomSeedBuilder)
            .setNegativeSampleWeight(trainConfig.negativeSampleWeight())
            .setFeaturePropertiesConfig(serializableFeaturePropertiesConfig(trainConfig));

        var projectedFeatureDimensionBuilder = TrainConfigsProto.ProjectedFeatureDimension
            .newBuilder()
            .setPresent(trainConfig.projectedFeatureDimension().isPresent());
        trainConfig.projectedFeatureDimension().ifPresent(projectedFeatureDimensionBuilder::setValue);
        protoConfigBuilder.setProjectedFeatureDimension(projectedFeatureDimensionBuilder);

        serializableRelationshipWeightConfig(trainConfig, protoConfigBuilder::setRelationshipWeightConfig);

        return protoConfigBuilder.build();
    }

    @Override
    public GraphSageTrainConfig fromSerializable(TrainConfigsProto.GraphSageTrainConfig protoTrainConfig) {
        var trainConfigBuilder =  GraphSageTrainConfig.builder()
            .modelName(protoTrainConfig.getModelConfig().getModelName())
            .embeddingDimension(protoTrainConfig.getEmbeddingDimensionConfig().getEmbeddingDimension())
            .aggregator(Aggregator.AggregatorType.of(protoTrainConfig.getAggregator().name()))
            .activationFunction(ActivationFunction.of(protoTrainConfig.getActivationFunction().name()))
            .sampleSizes(protoTrainConfig.getSampleSizesList())
            .batchSize(protoTrainConfig.getBatchSizeConfig().getBatchSize())
            .tolerance(protoTrainConfig.getToleranceConfig().getTolerance())
            .learningRate(protoTrainConfig.getLearningRate())
            .epochs(protoTrainConfig.getEpochs())
            .maxIterations(protoTrainConfig.getIterationsConfig().getMaxIterations())
            .negativeSampleWeight(protoTrainConfig.getNegativeSampleWeight())
            .featureProperties(protoTrainConfig.getFeaturePropertiesConfig().getFeaturePropertiesList());

        var randomSeed = protoTrainConfig.getRandomSeed();
        if (randomSeed.getPresent()) {
            trainConfigBuilder.randomSeed(randomSeed.getValue());
        }

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

    @Override
    public Class<TrainConfigsProto.GraphSageTrainConfig> serializableClass() {
        return TrainConfigsProto.GraphSageTrainConfig.class;
    }
}
