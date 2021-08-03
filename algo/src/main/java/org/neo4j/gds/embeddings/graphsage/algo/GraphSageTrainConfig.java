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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.BatchSizeConfig;
import org.neo4j.graphalgo.config.EmbeddingDimensionConfig;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.IterationsConfig;
import org.neo4j.graphalgo.config.ModelConfig;
import org.neo4j.graphalgo.config.RandomSeedConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.ToleranceConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
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
    FeaturePropertiesConfig,
    RandomSeedConfig {

    long serialVersionUID = 0x42L;

    @Override
    @Value.Default
    default int embeddingDimension() {
        return 64;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    @Configuration.ConvertWith("convertToIntSamples")
    default List<Integer> sampleSizes() {
        return List.of(25, 10);
    }

    static List<Integer> convertToIntSamples(List<Number> input) {
        try {
            return input.stream()
                .map(Number::longValue)
                .map(Math::toIntExact)
                .collect(Collectors.toList());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Sample size must smaller than 2^31", e);
        }
    }

    @Value.Default
    @Configuration.ConvertWith("org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.Aggregator.AggregatorType#toString")
    default Aggregator.AggregatorType aggregator() {
        return Aggregator.AggregatorType.MEAN;
    }

    @Value.Default
    @Configuration.ConvertWith("org.neo4j.gds.embeddings.graphsage.ActivationFunction#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.graphsage.ActivationFunction#toString")
    default ActivationFunction activationFunction() {
        return ActivationFunction.SIGMOID;
    }

    @Override
    @Value.Default
    default double tolerance() {
        return 1e-4;
    }

    @Value.Default
    default double learningRate() {
        return 0.1;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int epochs() {
        return 1;
    }

    @Override
    @Value.Default
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

    @Configuration.IntegerRange(min = 1)
    Optional<Integer> projectedFeatureDimension();

    @Override
    @Configuration.Ignore
    default boolean propertiesMustExistForEachNodeLabel() {
        return false;
    }

    @Configuration.Ignore
    @Value.Derived
    default boolean isWeighted() {
        return relationshipWeightProperty() != null;
    }

    @Configuration.Ignore
    default List<LayerConfig> layerConfigs(int featureDimension) {
        List<LayerConfig> result = new ArrayList<>(sampleSizes().size());

        Random random = new Random();
        randomSeed().ifPresent(random::setSeed);

        for (int i = 0; i < sampleSizes().size(); i++) {
            LayerConfig layerConfig = LayerConfig.builder()
                .aggregatorType(aggregator())
                .activationFunction(activationFunction())
                .rows(embeddingDimension())
                .cols(i == 0 ? featureDimension : embeddingDimension())
                .sampleSize(sampleSizes().get(i))
                .randomSeed(random.nextLong())
                .build();

            result.add(layerConfig);
        }

        return result;
    }

    @Configuration.Ignore
    default boolean isMultiLabel() {
        return projectedFeatureDimension().isPresent();
    }

    @Configuration.Ignore
    default int estimationFeatureDimension() {
        return projectedFeatureDimension().orElse(featureProperties().size());
    }

    @Value.Check
    default void validate() {
        if (featureProperties().isEmpty()) {
            throw new IllegalArgumentException(
                "GraphSage requires at least one property."
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

    static ImmutableGraphSageTrainConfig.Builder builder() {
        return ImmutableGraphSageTrainConfig.builder();
    }

    @Configuration.Ignore
    default void validateAgainstGraphStore(GraphStore graphStore) {
        var nodeLabels = this.nodeLabelIdentifiers(graphStore);
        var nodePropertyNames = this.featureProperties();

        if (this.isMultiLabel()) {
            // each property exists on at least one label
            var allProperties =
                graphStore
                    .schema()
                    .nodeSchema()
                    .allProperties();
            var missingProperties = nodePropertyNames
                .stream()
                .filter(key -> !allProperties.contains(key))
                .collect(Collectors.toSet());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Each property set in `featureProperties` must exist for at least one label. Missing properties: %s",
                    missingProperties
                ));
            }
        } else {
            // all properties exist on all labels
            List<String> missingProperties = nodePropertyNames
                .stream()
                .filter(weightProperty -> !graphStore.hasNodeProperty(nodeLabels, weightProperty))
                .collect(Collectors.toList());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The following node properties are not present for each label in the graph: %s. Properties that exist for each label are %s",
                    missingProperties,
                    graphStore.nodePropertyKeys(nodeLabels)
                ));
            }
        }
    }
}
