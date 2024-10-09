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
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.ActivationFunctionType;
import org.neo4j.gds.embeddings.graphsage.AggregatorType;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.embeddings.graphsage.TrainConfigTransformer;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminatedException;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.gds.TestGdsVersion.testGdsVersion;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class SingleLabelGraphSageTrainTest {

    @GdlGraph
    private static final String DB_CYPHER =         " CREATE" +
        "  (n0:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n1:Restaurant {dummyProp: 5.0, numEmployees: 2.0,   rating: 5.0, embedding: [1.0, 42.42] })" +
        ", (n4:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n5:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +
        ", (n6:Dish       {dummyProp: 5.0, numIngredients: 5.0, rating: 5.0})" +

        ", (n0)-[:SERVES { times: 5 }]->(n4)" +
        ", (n4)-[:REPLACES { times: 5 }]->(n5)" +
        ", (n5)-[:REPLACES { times: 5 }]->(n6)";

    @Inject
    private GraphStore graphStore;

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
    private TestGraph missingArrayGraph;

    @Inject
    private TestGraph missingArray2Graph;

    @Inject
    private TestGraph unequalGraph;

    @Test
    void shouldStoreMultiLabelFeatureFunctionInModel() {
        Graph weightedGraph = graphStore.getGraph(NodeLabel.of("Dish"), RelationshipType.of("REPLACES"), Optional.empty());
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of( "numIngredients", "rating"))
            .modelName("foo")
            .relationshipWeightProperty("times")
            .build();

        var SingleLabelGraphSageTrain = new SingleLabelGraphSageTrain(
            weightedGraph,
            TrainConfigTransformer.toParameters(config),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            config
        );
        // should not fail
        var model = SingleLabelGraphSageTrain.compute();
        assertThat(model.data().featureFunction()).isExactlyInstanceOf(SingleLabelFeatureFunction.class);

    }

    @Test
    void runsTrainingOnMultiLabelGraph() {
        String modelName = "gsModel";

        Graph weightedGraph = graphStore.getGraph(NodeLabel.of("Dish"), RelationshipType.of("REPLACES"), Optional.empty());

        var graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .concurrency(1)
            .featureProperties(List.of("numIngredients", "rating"))
            .aggregator(AggregatorType.MEAN)
            .activationFunction(ActivationFunctionType.SIGMOID)
            .embeddingDimension(64)
            .modelName(modelName)
            .relationshipWeightProperty("times")
            .build();

        var graphSageTrain = new SingleLabelGraphSageTrain(
            weightedGraph,
            TrainConfigTransformer.toParameters(graphSageTrainConfig),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            graphSageTrainConfig
        );

        var model = graphSageTrain.compute();

        assertEquals(modelName, model.name());
        assertEquals(GraphSage.MODEL_TYPE, model.algoType());

        GraphSageTrainConfig trainConfig = model.trainConfig();
        assertNotNull(trainConfig);
        assertEquals(1, trainConfig.concurrency().value());
        assertEquals(List.of("numIngredients", "rating"), trainConfig.featureProperties());
        assertEquals("MEAN", AggregatorType.toString(trainConfig.aggregator()));
        assertEquals("SIGMOID", ActivationFunctionType.toString(trainConfig.activationFunction()));
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
        int featureDimension = 10;
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("p1", "p2"))
            .embeddingDimension(64)
            .modelName("foo")
            .projectedFeatureDimension(featureDimension)
            .build();

        var SingleLabelGraphSageTrain = new SingleLabelGraphSageTrain(
            unequalGraph,
            TrainConfigTransformer.toParameters(config),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            config
        );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(SingleLabelGraphSageTrain::compute)
            .withMessageContaining(
                "The property `p2` contains arrays of differing lengths `1` and `2`."
            );
    }

    void shouldFailMissingArrayProperty(Graph graph, String property, long missingNode) {
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("p1", "p2"))
            .embeddingDimension(64)
            .modelName("foo")
            .projectedFeatureDimension(10)
            .build();

        var SingleLabelGraphSageTrain = new SingleLabelGraphSageTrain(
            graph,
            TrainConfigTransformer.toParameters(config),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            config
        );
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(SingleLabelGraphSageTrain::compute)
            .withMessageContaining(
                formatWithLocale("Missing node property for property key `%s` on node with id `%d`.", property, missingNode)
            );

    }

    private static Stream<Arguments> featureSizes() {
        var builder = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases", "embedding"))
            .embeddingDimension(64)
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

    @Test
    void testTermination() {
        String modelName = "gsModel";
        Graph weightedGraph = graphStore.getGraph(NodeLabel.of("Dish"), RelationshipType.of("REPLACES"), Optional.empty());

        var graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
            .modelUser("")
            .concurrency(1)
            .featureProperties(List.of("rating"))
            .aggregator(AggregatorType.MEAN)
            .activationFunction(ActivationFunctionType.SIGMOID)
            .embeddingDimension(64)
            .modelName(modelName)
            .relationshipWeightProperty("times")
            .build();

        var graphSageTrain = new SingleLabelGraphSageTrain(
            weightedGraph,
            TrainConfigTransformer.toParameters(graphSageTrainConfig),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.STOP_RUNNING,
            testGdsVersion,
            graphSageTrainConfig
        );

        assertThatThrownBy(graphSageTrain::compute)
            .isInstanceOf(TerminatedException.class)
            .hasMessageContaining("The execution has been terminated.");
    }
}
