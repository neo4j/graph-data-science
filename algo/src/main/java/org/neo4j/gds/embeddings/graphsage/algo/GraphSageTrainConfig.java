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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.EmbeddingDimensionConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.ToleranceConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.ml.training.TrainBaseConfig;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Configuration("GraphSageTrainConfigImpl")
public interface GraphSageTrainConfig extends
    TrainBaseConfig,
    BatchSizeConfig,
    IterationsConfig,
    ToleranceConfig,
    EmbeddingDimensionConfig,
    RelationshipWeightConfig,
    FeaturePropertiesConfig,
    RandomSeedConfig {

    long serialVersionUID = 0x42L;

    @Override
    List<String> featureProperties();

    @Override
    default int embeddingDimension() {
        return 64;
    }

    @Configuration.IntegerRange(min = 1)
    @Configuration.ConvertWith(method = "convertToIntSamples")
    default List<Integer> sampleSizes() {
        return List.of(25, 10);
    }

    static List<Integer> convertToIntSamples(List<? extends Number> input) {
        try {
            return input.stream()
                .map(Number::longValue)
                .map(Math::toIntExact)
                .collect(Collectors.toList());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Sample size must smaller than 2^31", e);
        }
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#toString")
    default Aggregator.AggregatorType aggregator() {
        return Aggregator.AggregatorType.MEAN;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.embeddings.graphsage.ActivationFunction#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.ActivationFunction#toString")
    default ActivationFunction activationFunction() {
        return ActivationFunction.SIGMOID;
    }

    @Override
    default double tolerance() {
        return 1e-4;
    }

    default double learningRate() {
        return 0.1;
    }

    @Configuration.IntegerRange(min = 1)
    default int epochs() {
        return 1;
    }

    @Override
    default int maxIterations() {
        return 10;
    }

    @Configuration.Key("batchSamplingRatio")
    @Configuration.DoubleRange(min = 0, max = 1, minInclusive = false)
    Optional<Double> maybeBatchSamplingRatio();

    @Configuration.DoubleRange(min = 0)
    default double penaltyL2() {
        return 0;
    }

    default int searchDepth() {
        return 5;
    }

    default int negativeSampleWeight() {
        return 20;
    }

    @Configuration.IntegerRange(min = 1)
    Optional<Integer> projectedFeatureDimension();

    @Override
    @Configuration.Ignore
    default boolean propertiesMustExistForEachNodeLabel() {
        return !isMultiLabel();
    }

    @Configuration.Ignore
    default boolean isMultiLabel() {
        return projectedFeatureDimension().isPresent();
    }

    @Configuration.Check
    default void validate() {
        if (featureProperties().isEmpty()) {
            throw new IllegalArgumentException(
                "GraphSage requires at least one property."
            );
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNonEmptyGraph(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (selectedRelationshipTypes.stream().mapToLong(graphStore::relationshipCount).sum() == 0) {
            throw new IllegalArgumentException("There should be at least one relationship in the graph.");
        }
    }

    static GraphSageTrainConfig of(String username, CypherMapWrapper userInput) {
        return new GraphSageTrainConfigImpl(username, userInput);
    }


    @Configuration.Ignore
    default GraphSageTrainMemoryEstimateParameters toMemoryEstimateParameters() {
        var estimationFeatureDimension = projectedFeatureDimension().orElse(featureProperties().size());
        var layerConfigs = GraphSageHelper.layerConfigs(estimationFeatureDimension, sampleSizes(), randomSeed(), aggregator(), activationFunction(), embeddingDimension());
        return new GraphSageTrainMemoryEstimateParameters(
            layerConfigs,
            isMultiLabel(),
            featureProperties().size(),
            estimationFeatureDimension,
            batchSize(),
            embeddingDimension()
        );
    }

    @Configuration.Ignore
    default GraphSageTrainParameters toParameters() {
        return new GraphSageTrainParameters(
            concurrency(),
            batchSize(),
            maxIterations(),
            searchDepth(),
            epochs(),
            learningRate(),
            tolerance(),
            negativeSampleWeight(),
            penaltyL2(),
            embeddingDimension(),
            sampleSizes(),
            featureProperties(),
            maybeBatchSamplingRatio(),
            randomSeed(),
            aggregator(),
            activationFunction()
        );
    }
}
