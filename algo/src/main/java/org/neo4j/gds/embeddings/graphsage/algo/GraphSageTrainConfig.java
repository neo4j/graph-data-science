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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.BatchSizeConfig;
import org.neo4j.gds.config.EmbeddingDimensionConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.ToleranceConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.LayerConfig;
import org.neo4j.gds.model.ModelConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

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

    @Configuration.Key("batchSamplingRatio")
    @Configuration.DoubleRange(min = 0, max = 1, minInclusive = false)
    Optional<Double> maybeBatchSamplingRatio();

    @Configuration.Ignore
    @Value.Derived
    default int batchesPerIteration(long nodeCount) {
        var samplingRatio = maybeBatchSamplingRatio().orElse(Math.min(1.0, batchSize() * concurrency() / (double) nodeCount));
        var totalNumberOfBatches = Math.ceil(nodeCount / (double) batchSize());
        return (int) Math.ceil(samplingRatio * totalNumberOfBatches);
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

    static GraphSageTrainConfig of(String username, CypherMapWrapper userInput) {
        return new GraphSageTrainConfigImpl(username, userInput);
    }

    @TestOnly
    static ImmutableGraphSageTrainConfig.Builder testBuilder() {
        return ImmutableGraphSageTrainConfig.builder();
    }
}
