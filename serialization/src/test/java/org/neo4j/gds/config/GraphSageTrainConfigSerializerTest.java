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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSageTrainConfigSerializerTest {

    @ParameterizedTest
    @MethodSource("aggregatorsWithActivationFunctions")
    void testRoundTrip(Aggregator.AggregatorType aggregator, ActivationFunction activationFunction) {
        var trainConfigBuilder = ImmutableGraphSageTrainConfig.builder()
            .modelName("MODEL_NAME")
            .aggregator(aggregator)
            .randomSeed(19L)
            .activationFunction(activationFunction)
            .featureProperties(List.of("age", "birth_year", "death_year"));

        var trainConfig = trainConfigBuilder.build();
        var trainConfigSerializer = new GraphSageTrainConfigSerializer();
        var proto = trainConfigSerializer.toSerializable(trainConfig);
        assertThat(proto).isNotNull();

        var from = trainConfigSerializer.fromSerializable(proto);
        assertThat(from)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(trainConfig);
    }

    @ParameterizedTest
    @MethodSource("aggregatorsWithActivationFunctions")
    void testRoundTripWeightedMultiLabel(Aggregator.AggregatorType aggregator, ActivationFunction activationFunction) {
        var trainConfigBuilder = ImmutableGraphSageTrainConfig.builder()
            .modelName("MODEL_NAME")
            .aggregator(aggregator)
            .activationFunction(activationFunction)
            .featureProperties(List.of("age", "birth_year", "death_year"))
            .projectedFeatureDimension(1000)
            .relationshipWeightProperty("blah-blah");

        var trainConfig = trainConfigBuilder.build();
        var trainConfigSerializer = new GraphSageTrainConfigSerializer();

        var proto = trainConfigSerializer.toSerializable(trainConfig);
        assertThat(proto).isNotNull();

        var from = trainConfigSerializer.fromSerializable(proto);
        assertThat(from)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(trainConfig);
    }

    @ParameterizedTest
    @MethodSource("nonDefaultParameters")
    void testRoundTripWithNonDefaultParameters(
        int embeddingDimension,
        List<Integer> sampleSizes,
        double tolerance,
        double learningRate,
        int epochs,
        int maxIterations,
        int negativeSampleWeight,
        int batchSize
    ) {
        var trainConfigBuilder = ImmutableGraphSageTrainConfig.builder()
            .modelName("MODEL_NAME")
            .embeddingDimension(embeddingDimension)
            .sampleSizes(sampleSizes)
            .tolerance(tolerance)
            .learningRate(learningRate)
            .epochs(epochs)
            .maxIterations(maxIterations)
            .negativeSampleWeight(negativeSampleWeight)
            .featureProperties(List.of("age", "birth_year", "death_year"))
            .batchSize(batchSize);

        var trainConfig = trainConfigBuilder.build();
        var trainConfigSerializer = new GraphSageTrainConfigSerializer();
        var proto = trainConfigSerializer.toSerializable(trainConfig);
        assertThat(proto).isNotNull();

        var from = trainConfigSerializer.fromSerializable(proto);
        assertThat(from)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(trainConfig);
    }

    private static Stream<Arguments> aggregatorsWithActivationFunctions() {
        return org.neo4j.graphalgo.TestSupport.crossArguments(
            () -> Stream.of(Arguments.of(Aggregator.AggregatorType.MEAN), Arguments.of(Aggregator.AggregatorType.POOL)),
            () -> Stream.of(Arguments.of(ActivationFunction.RELU), Arguments.of(ActivationFunction.RELU))
        );
    }

    private static Stream<Arguments> nonDefaultParameters() {
        return org.neo4j.graphalgo.TestSupport.crossArguments(
            () -> Stream.of(Arguments.of(512)),                         // embeddingDimension
            () -> Stream.of(Arguments.of(List.of(42, 1337))),         // sampleSizes
            () -> Stream.of(Arguments.of(10.1), Arguments.of(0.8)),     // tolerance
            () -> Stream.of(Arguments.of(100.1), Arguments.of(0.18)),   // learningRate
            () -> Stream.of(Arguments.of(100), Arguments.of(1000)),     // epochs
            () -> Stream.of(Arguments.of(200), Arguments.of(10)),       // maxIterations
            () -> Stream.of(Arguments.of(250), Arguments.of(20)),       // negativeSampleWeight
            () -> Stream.of(Arguments.of(1500), Arguments.of(40))       // batchSize

        );
    }

}
