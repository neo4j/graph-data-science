/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.immutables.value.Value;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.BatchSizeConfig;
import org.neo4j.graphalgo.config.EmbeddingDimensionConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.IterationsConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.ToleranceConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.graphalgo.utils.StringJoining;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration("GraphSageTrainConfigImpl")
@SuppressWarnings("immutables:subtype")
public interface GraphSageTrainConfig extends
    AlgoBaseConfig,
    ModelConfig,
    BatchSizeConfig,
    IterationsConfig,
    ToleranceConfig,
    EmbeddingDimensionConfig,
    RelationshipWeightConfig,
    FeaturePropertiesConfig {

    int PROJECTED_FEATURE_DIMENSION = -1;

    @Override
    @Value.Default
    default int embeddingDimension() {
        return 64;
    }

    @Value.Default
    default List<Long> sampleSizes() {
        return List.of(25L, 10L);
    }

    @Configuration.ConvertWith("org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#toString")
    @Value.Default
    default Aggregator.AggregatorType aggregator() {
        return Aggregator.AggregatorType.MEAN;
    }

    @Configuration.ConvertWith("org.neo4j.gds.embeddings.graphsage.ActivationFunction#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.ActivationFunction#toString")
    @Value.Default
    default ActivationFunction activationFunction() {
        return ActivationFunction.SIGMOID;
    }

    @Value.Default
    @Override
    default double tolerance() {
        return 1e-4;
    }

    @Value.Default
    default double learningRate() {
        return 0.1;
    }

    @Value.Default
    default int epochs() {
        return 1;
    }

    @Value.Default
    @Override
    default int maxIterations() {
        return 10;
    }

    @Value.Default
    default int searchDepth() {
        return 5;
    }

    @Value.Default
    default int negativeSampleWeight() {
        return 20;
    }

    @Value.Default
    default boolean degreeAsProperty() {
        return false;
    }

    // TODO: Can use Optional
    @Value.Default
    default int projectedFeatureDimension() {
        return PROJECTED_FEATURE_DIMENSION;
    }

    @Override
    @Configuration.Ignore
    default boolean propertiesMustExistForEachNodeLabel() {
        return false;
    }

    @Configuration.Ignore
    default List<LayerConfig> layerConfigs() {
        List<LayerConfig> result = new ArrayList<>(sampleSizes().size());
        for (int i = 0; i < sampleSizes().size(); i++) {
            LayerConfig layerConfig = LayerConfig.builder()
                .aggregatorType(aggregator())
                .activationFunction(activationFunction())
                .rows(embeddingDimension())
                .cols(i == 0 ? featuresSize() : embeddingDimension())
                .sampleSize(sampleSizes().get(i))
                .build();

            result.add(layerConfig);
        }

        return result;
    }

    @Configuration.Ignore
    default boolean isMultiLabel() {
        return projectedFeatureDimension() > 0;
    }

    @Configuration.Ignore
    default int featuresSize() {
        return isMultiLabel()
            ? projectedFeatureDimension()
            : featureProperties().size() + (degreeAsProperty() ? 1 : 0);
    }

    @Value.Check
    default void validate() {
        if (featureProperties().isEmpty() && !degreeAsProperty()) {
            throw new IllegalArgumentException(
                "GraphSage requires at least one property. Either `featureProperties` or `degreeAsProperty` must be set."
            );
        }
    }

    static GraphSageTrainConfig of(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new GraphSageTrainConfigImpl(
            graphName,
            maybeImplicitCreate,
            username,
            userInput
        );
    }

    static GraphSageTrainConfig of(
        String modelName,
        ActivationFunction activationFunction,
        Aggregator.AggregatorType aggregator,
        int batchSize,
        int embeddingDimension,
        List<String> featureProperties,
        double tolerance
    ) {
        return ImmutableGraphSageTrainConfig.builder()
            .modelName(modelName)
            .activationFunction(activationFunction)
            .aggregator(aggregator)
            .batchSize(batchSize)
            .embeddingDimension(embeddingDimension)
            .featureProperties(featureProperties)
            .tolerance(tolerance)
            .build();
    }

    @Configuration.Ignore
    default void validateAgainstGraphStore(GraphStoreWithConfig graphStoreWithConfig) {
        var graphStore = graphStoreWithConfig.graphStore();
        var nodeLabels = this.nodeLabelIdentifiers(graphStore);
        var nodePropertyNames = this.featureProperties();
        if (!this.isMultiLabel()) {
            // all properties exist on all labels
            List<String> missingProperties = nodePropertyNames
                .stream()
                .filter(weightProperty -> !graphStore.hasNodeProperty(nodeLabels, weightProperty))
                .collect(Collectors.toList());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node properties %s not found in graph with node properties: %s in all node labels: %s",
                    missingProperties,
                    graphStore.nodePropertyKeys(nodeLabels),
                    StringJoining.join(nodeLabels.stream().map(NodeLabel::name))
                ));
            }
        } else {
            // each property exists on at least one label
            var allProperties =
                graphStore.nodePropertyKeys()
                    .entrySet()
                    .stream()
                    .filter(entry -> nodeLabels.contains(entry.getKey()))
                    .flatMap(entry -> entry.getValue().stream())
                    .collect(Collectors.toSet());
            var missingProperties = nodePropertyNames
                .stream()
                .filter(key -> !allProperties.contains(key))
                .collect(Collectors.toSet());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Each property set in `featureProperties` must exist for one label. Missing properties: %s",
                    missingProperties
                ));
            }

            var labelsWithoutProperties = nodeLabels
                .stream()
                .filter(nodeLabel -> graphStore
                    .nodePropertyKeys(nodeLabel)
                    .stream()
                    .noneMatch(nodePropertyNames::contains))
                .map(NodeLabel::name)
                .collect(Collectors.toSet());
            if (!labelsWithoutProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Each label must have at least one of the properties set in `featureProperties`. Labels with missing properties: %s",
                    StringJoining.join(labelsWithoutProperties)
                ));
            }
        }
    }
}
