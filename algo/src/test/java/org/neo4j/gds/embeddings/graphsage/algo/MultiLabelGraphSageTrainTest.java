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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.MultiLabelFeatureFunction;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class MultiLabelGraphSageTrainTest {

    private static final int PROJECTED_FEATURE_SIZE = 5;

    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRunWithOrWithoutDegreeAsProperty(boolean degreeAsProperty) {
        var config = ImmutableGraphSageTrainConfig.builder()
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(64)
            .modelName("foo")
            .degreeAsProperty(degreeAsProperty)
            .projectedFeatureSize(PROJECTED_FEATURE_SIZE)
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );
        // should not fail
        multiLabelGraphSageTrain.compute();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("featureSizes")
    void shouldRunWithDifferentProjectedFeatureSizes(String name, GraphSageTrainConfig config) {
        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );
        // should not fail
        multiLabelGraphSageTrain.compute();
    }

    @Test
    void shouldStoreMultiLabelFeatureFunctionInModel() {
        var config = ImmutableGraphSageTrainConfig.builder()
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .modelName("foo")
            .projectedFeatureSize(PROJECTED_FEATURE_SIZE)
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );
        // should not fail
        Model<ModelData, GraphSageTrainConfig> model = multiLabelGraphSageTrain.compute();
        assertThat(model.data().featureFunction()).isExactlyInstanceOf(MultiLabelFeatureFunction.class);

    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        String modelName = "gsModel";

        var graphSageTrainConfig = ImmutableGraphSageTrainConfig.builder()
            .concurrency(1)
            .projectedFeatureSize(5)
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .aggregator(Aggregator.AggregatorType.MEAN)
            .activationFunction(ActivationFunction.SIGMOID)
            .embeddingDimension(64)
            .degreeAsProperty(true)
            .modelName(modelName)
            .build();

        var graphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            graphSageTrainConfig,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        var model = graphSageTrain.compute();

        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency());
        assertEquals(List.of("numEmployees", "numIngredients", "rating", "numPurchases"), trainConfig.nodePropertyNames());
        assertEquals("MEAN", Aggregator.AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunction.toString(trainConfig.activationFunction()));
        assertEquals(64, trainConfig.embeddingDimension());
        assertTrue(trainConfig.degreeAsProperty());
    }

    private static Stream<Arguments> featureSizes() {
        var builder = ImmutableGraphSageTrainConfig.builder()
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(64)
            .projectedFeatureSize(PROJECTED_FEATURE_SIZE)
            .modelName("foo");
        return Stream.of(
            Arguments.of(
                "default", builder.build()
            ), Arguments.of(
                "larger projection", builder.projectedFeatureSize(10).build()
            ), Arguments.of(
                "smaller projection", builder.projectedFeatureSize(2).build()
            )
        );

    }
}
