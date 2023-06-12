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
package org.neo4j.gds.embeddings.graphsage;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfigImpl;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.core.features.FeatureExtractionBaseTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class GraphSageHelperTest {

    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    private TestGraph graph;

    @InjectSoftAssertions
    private SoftAssertions soft;

    @ParameterizedTest(name = "{0}")
    @MethodSource({"parameters"})
    void shouldInitializeFeaturesCorrectly(String name, GraphSageTrainConfig config, Map<String, double[]> expected) {
        var actual = config.isMultiLabel() ? GraphSageHelper.initializeMultiLabelFeatures(
            graph,
            GraphSageHelper.multiLabelFeatureExtractors(graph, config)
        ) : GraphSageHelper.initializeSingleLabelFeatures(graph, config);

        soft.assertThat(actual.size()).isEqualTo(expected.size());

        expected.forEach((node, expectedFeatures) -> {
            var actualFeatures = actual.get(graph.toMappedNodeId(node));
            soft.assertThat(actualFeatures)
                .withFailMessage(formatWithLocale("Node %s has unexpected features. Expected %s, but got %s.", node,
                    Arrays.toString(expectedFeatures), Arrays.toString(actualFeatures)))
                .containsExactly(expectedFeatures);
        });
    }

    @Test
    void shouldValidateSingleLabelPerNode() {
        var graph = GdlFactory.of("(:Foo:Bar)").build().getUnion();
        var config = GraphSageTrainConfigImpl.builder()
            .modelName("foo")
            .modelUser("")
            .featureProperties(List.of("dummyProp"))
            .projectedFeatureDimension(42)
            .build();
        var exception = assertThrows(IllegalArgumentException.class, () ->
            GraphSageHelper.initializeMultiLabelFeatures(graph,
                GraphSageHelper.multiLabelFeatureExtractors(graph, config)
            )
        );
        assertThat(exception).hasMessage(
            "Each node must have exactly one label: nodeId=0, labels=[NodeLabel{name='Bar'}, NodeLabel{name='Foo'}]"
        );
    }

    static Stream<Arguments> parameters() {
        var multiLabelFeatures = new HashMap<>();

        for (int i = 0; i < 4; i++) {
            multiLabelFeatures.put("n" + i, new double[]{2.0, 5.0, 1.0});
        }

        for (int i = 4; i < 13; i++) {
            multiLabelFeatures.put("n" + i, new double[]{5.0, 5.0, 1.0});
        }

        for (int i = 13; i < 20; i++) {
            multiLabelFeatures.put("n" + i, new double[]{5.0, 1.0});
        }

        return Stream.of(
            Arguments.of(
                "single label",
                GraphSageTrainConfigImpl.builder()
                    .modelName("foo")
                    .featureProperties(List.of("dummyProp"))
                    .modelUser("")
                    .build(),
                IntStream.range(0, 20).mapToObj(i -> "n" + i).collect(Collectors.toMap(
                    s -> s,
                    s -> new double[]{5.0}
                ))
            ), Arguments.of(
                "multi label",
                GraphSageTrainConfigImpl.builder()
                    .modelName("foo")
                    .modelUser("")
                    .featureProperties(List.of("numEmployees", "rating", "numIngredients", "numPurchases"))
                    .projectedFeatureDimension(5)
                    .build(),
                multiLabelFeatures
            )
        );
    }

    @Nested
    class FailingFeatureExtraction extends FeatureExtractionBaseTest {

        @Override
        public void makeExtractions(Graph graph) {
            GraphSageTrainConfig graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
                .modelName("foo")
                .modelUser("")
                .featureProperties(List.of("a", "b"))
                .build();

            GraphSageHelper.initializeSingleLabelFeatures(graph, graphSageTrainConfig);
        }
    }

    @Nested
    class ArrayProperties {
        @GdlGraph(graphNamePrefix = "valid")
        private static final String VALID_GRAPH = "CREATE " +
                                                  "  (a { prop: 1.4, arrayProp: [-1.1,2.5] })" +
                                                  ", (b { prop: 1.8, arrayProp: [1.0,2.0] })" +
                                                  ", (a)-[:REL]->(b)";

        @Inject
        TestGraph validGraph;

        @Test
        void shouldConcatenateFeatures() {
            GraphSageTrainConfig graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
                .modelName("foo")
                .modelUser("")
                .featureProperties(List.of("prop", "arrayProp"))
                .build();

            var features = GraphSageHelper.initializeSingleLabelFeatures(
                validGraph,
                graphSageTrainConfig
            );
            //TODO: check where rounding error is coming from
            assertThat(features.get(validGraph.toMappedNodeId("a"))).contains(new double[] {1.4, -1.1, 2.5}, Offset.offset(1e-6));
            assertThat(features.get(validGraph.toMappedNodeId("b"))).contains(new double[] {1.8, 1.0, 2.0}, Offset.offset(1e-6));
        }
    }

    @Nested
    class MissingProperties {

        @GdlGraph
        private static final String DB_CYPHER = "CREATE " +
                                                "  (a { prop: 1 })" +
                                                ", (b)" +
                                                ", (a)-[:REL]->(b)";

        @Inject
        Graph graph;

        @Inject
        IdFunction idFunction;

        @Test
        void shouldThrowOnMissingProperties() {
            GraphSageTrainConfig graphSageTrainConfig = GraphSageTrainConfigImpl.builder()
                .modelName("foo")
                .modelUser("foo")
                .featureProperties(List.of("prop"))
                .build();

            assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> GraphSageHelper.initializeSingleLabelFeatures(
                    graph,
                    graphSageTrainConfig
                ))
                .withMessageContaining(
                    formatWithLocale("Node with ID `%s` has invalid feature property value `NaN` for property `prop`", idFunction.of("b"))
                );
        }
    }

    @Nested
    class LongPropertyValues {

        @GdlGraph
        private static final String DB_CYPHER =
            " CREATE" +
            "  (n0:Restaurant {dummyProp: 5, numEmployees: 2.0, rating: 7})" +
            ", (n1:Pub {dummyProp: 5, numEmployees: 2, rating: 7.0})" +
            ", (n2:Cafe {dummyProp: 5.0, numEmployees: 2, rating: 7})";

        @Test
        void shouldInitializeFeaturesCorrectly() {
            var config = GraphSageTrainConfigImpl.builder()
                .modelName("foo")
                .modelUser("")
                .featureProperties(List.of("dummyProp", "numEmployees", "rating"))
                .build();

            var actual = GraphSageHelper.initializeSingleLabelFeatures(graph, config);

            var expected = HugeObjectArray.newArray(double[].class, 3);
            expected.setAll(i -> new double[] {5.0, 2.0, 7.0});

            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactlyInAnyOrder(expected.get(i));
            }
        }
    }
}
