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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.Aggregator;
import org.neo4j.gds.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.gds.embeddings.graphsage.MultiLabelFeatureFunction;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.gds.TestGdsVersion.testGdsVersion;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class MultiLabelGraphSageTrainTest {

    private static final int PROJECTED_FEATURE_SIZE = 5;

    @GdlGraph(graphNamePrefix = "weighted")
    private static final String GDL = GraphSageTestGraph.GDL;

    @GdlGraph(graphNamePrefix = "missingArray")
    public static final String MISSING_ARRAY_GRAPH =
        " CREATE" +
        "  (n0:A {p1: 2.0})" +
        "  (n1:A {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n2:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n3:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n0)-[:T]->(n1)";

    @GdlGraph(graphNamePrefix = "missingArray2")
    public static final String MISSING_ARRAY_GRAPH2 =
        " CREATE" +
        "  (n0:A {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n1:A {p1: 2.0})" +
        "  (n2:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n3:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n0)-[:T]->(n1)";

    @GdlGraph(graphNamePrefix = "unequal")
    public static final String UNEQUAL_ARRAY_GRAPH =
        " CREATE" +
        "  (n0:A {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n1:A {p1: 2.0, p2: [2.0]})" +
        "  (n2:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n3:B {p1: 2.0, p2: [2.0, 2.0]})" +
        "  (n0)-[:T]->(n1)";

    @Inject
    TestGraph weightedGraph;

    @Inject
    TestGraph missingArrayGraph;

    @Inject
    TestGraph missingArray2Graph;

    @Inject
    TestGraph unequalGraph;

    @ParameterizedTest(name = "{0}")
    @MethodSource("featureSizes")
    void shouldRunWithDifferentProjectedFeatureSizes(String name, GraphSageTrainConfig config) {
        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            weightedGraph,
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            testGdsVersion
        );
        // should not fail
        multiLabelGraphSageTrain.compute();
    }

    @Test
    void shouldStoreMultiLabelFeatureFunctionInModel() {
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"))
            .modelName("foo")
            .projectedFeatureDimension(PROJECTED_FEATURE_SIZE)
            .relationshipWeightProperty("times")
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            weightedGraph,
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            testGdsVersion
        );
        // should not fail
        var model = multiLabelGraphSageTrain.compute();
        assertThat(model.data().featureFunction()).isExactlyInstanceOf(MultiLabelFeatureFunction.class);

    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        String modelName = "gsModel";

        var graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .concurrency(1)
            .projectedFeatureDimension(5)
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"))
            .aggregator(Aggregator.AggregatorType.MEAN)
            .activationFunction(ActivationFunction.SIGMOID)
            .embeddingDimension(64)
            .modelName(modelName)
            .relationshipWeightProperty("times")
            .build();

        var graphSageTrain = new MultiLabelGraphSageTrain(
            weightedGraph,
            graphSageTrainConfig,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            testGdsVersion
        );

        var model = graphSageTrain.compute();

        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency());
        assertEquals(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"), trainConfig.featureProperties());
        assertEquals("MEAN", Aggregator.AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunction.toString(trainConfig.activationFunction()));
        assertEquals(64, trainConfig.embeddingDimension());
    }

    @Test
    void shouldFailMissingArrayPropertyNode0() {
        shouldFailMissingArrayProperty(missingArrayGraph, "p2", missingArrayGraph.toOriginalNodeId("n0"));
    }

    @Test
    void shouldFailMissingArrayPropertyNode1() {
        shouldFailMissingArrayProperty(missingArray2Graph, "p2", missingArray2Graph.toOriginalNodeId("n1"));
    }

    @Test
    void shouldFailUnequalLengthArrays() {
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("p1", "p2"))
            .embeddingDimension(64)
            .projectedFeatureDimension(PROJECTED_FEATURE_SIZE)
            .modelName("foo")
            .projectedFeatureDimension(10)
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            unequalGraph,
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            testGdsVersion
        );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(multiLabelGraphSageTrain::compute)
            .withMessageContaining(
                "The property `p2` contains arrays of differing lengths `1` and `2`."
            );
    }

    void shouldFailMissingArrayProperty(Graph graph, String property, long missingNode) {
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("p1", "p2"))
            .embeddingDimension(64)
            .projectedFeatureDimension(PROJECTED_FEATURE_SIZE)
            .modelName("foo")
            .projectedFeatureDimension(10)
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            testGdsVersion
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(multiLabelGraphSageTrain::compute)
            .withMessageContaining(
                formatWithLocale("Missing node property for property key `%s` on node with id `%d`.", property, missingNode)
            );

    }

    private static Stream<Arguments> featureSizes() {
        var builder = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"))
            .embeddingDimension(64)
            .projectedFeatureDimension(PROJECTED_FEATURE_SIZE)
            .relationshipWeightProperty("times")
            .modelName("foo");
        return Stream.of(
            Arguments.of(
                "default", builder.build()
            ), Arguments.of(
                "larger projection", builder.projectedFeatureDimension(10).build()
            ), Arguments.of(
                "smaller projection", builder.projectedFeatureDimension(2).build()
            )
        );

    }
}
