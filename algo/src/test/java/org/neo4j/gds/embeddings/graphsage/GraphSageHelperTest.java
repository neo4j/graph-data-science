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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.core.ml.features.FeatureExtractionBaseTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
class GraphSageHelperTest {

    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    private Graph graph;

    @ParameterizedTest(name = "{0}")
    @MethodSource({"parameters"})
    void shouldInitializeFeaturesCorrectly(String name, GraphSageTrainConfig config, HugeObjectArray<double[]> expected) {

        var multiLabelFeatureExtractors = GraphSageHelper.multiLabelFeatureExtractors(graph, config);
        var actual = config.isMultiLabel() ? GraphSageHelper.initializeMultiLabelFeatures(
            graph,
            multiLabelFeatureExtractors, AllocationTracker.empty()
        ) : GraphSageHelper.initializeSingleLabelFeatures(graph, config, AllocationTracker.empty());

        assertEquals(expected.size(), actual.size());
        for(int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).containsExactlyInAnyOrder(expected.get(i));
        }
    }

    @Test
    void shouldValidateSingleLabelPerNode() {
        var graph = GdlFactory.of("(:Foo:Bar)").build().graphStore().getUnion();
        var config = ImmutableGraphSageTrainConfig.builder()
            .modelName("foo")
            .featureProperties(Set.of("dummyProp"))
            .projectedFeatureDimension(42)
            .build();
        var exception = assertThrows(IllegalArgumentException.class, () ->
            GraphSageHelper.initializeMultiLabelFeatures(graph,
                GraphSageHelper.multiLabelFeatureExtractors(graph, config),
                AllocationTracker.empty()
            )
        );
        assertThat(exception).hasMessage(
            "Each node must have exactly one label: nodeId=0, labels=[NodeLabel{name='Bar'}, NodeLabel{name='Foo'}]"
        );
    }

    static Stream<Arguments> parameters() {
        var singleLabelProperties = HugeObjectArray.newArray(
            double[].class,
            20,
            AllocationTracker.empty()
        );
        singleLabelProperties.fill(new double[]{5.0});

        return Stream.of(
            Arguments.of(
                "single label",
                ImmutableGraphSageTrainConfig.builder()
                    .modelName("foo")
                    .featureProperties(Set.of("dummyProp"))
                    .build(),
                    singleLabelProperties
            ), Arguments.of(
                "multi label",
                ImmutableGraphSageTrainConfig.builder()
                    .modelName("foo")
                    .featureProperties(Set.of("numEmployees", "rating", "numIngredients", "numPurchases"))
                    .projectedFeatureDimension(5)
                    .build(),
                HugeObjectArray.of(
                    new double[]{5.0, 2.0, 1.0},
                    new double[]{5.0, 2.0, 1.0},
                    new double[]{5.0, 2.0, 1.0},
                    new double[]{5.0, 2.0, 1.0},

                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},
                    new double[]{5.0, 5.0, 1.0},

                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0},
                    new double[]{5.0, 1.0}
                )
            )
        );
    }

    @Nested
    class FailingFeatureExtraction extends FeatureExtractionBaseTest {

        @Override
        public void makeExtractions(Graph graph) {
            GraphSageTrainConfig graphSageTrainConfig = ImmutableGraphSageTrainConfig.builder()
                .modelName("foo")
                .featureProperties(List.of("a", "b"))
                .build();

            GraphSageHelper.initializeSingleLabelFeatures(graph, graphSageTrainConfig, AllocationTracker.empty());
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
        Graph validGraph;

        @Inject
        IdFunction validIdFunction;

        @Test
        void shouldConcatenateFeatures() {
            GraphSageTrainConfig graphSageTrainConfig = ImmutableGraphSageTrainConfig.builder()
                .modelName("foo")
                .featureProperties(List.of("prop", "arrayProp"))
                .build();

            var features = GraphSageHelper.initializeSingleLabelFeatures(
                validGraph,
                graphSageTrainConfig,
                AllocationTracker.empty()
            );
            //TODO: check where rounding error is coming from
            assertThat(features.get(validIdFunction.of("a"))).contains(new double[] {1.4, -1.1, 2.5}, Offset.offset(1e-6));
            assertThat(features.get(validIdFunction.of("b"))).contains(new double[] {1.8, 1.0, 2.0}, Offset.offset(1e-6));
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
            GraphSageTrainConfig graphSageTrainConfig = ImmutableGraphSageTrainConfig.builder()
                .modelName("foo")
                .featureProperties(Set.of("prop"))
                .build();

            assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> GraphSageHelper.initializeSingleLabelFeatures(
                    graph,
                    graphSageTrainConfig,
                    AllocationTracker.empty()))
                .withMessageContaining(
                    formatWithLocale("Missing node property for property key `prop` on node with id `%s`.", idFunction.of("b"))
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
            var config = ImmutableGraphSageTrainConfig.builder()
                .modelName("foo")
                .featureProperties(Set.of("dummyProp", "numEmployees", "rating"))
                .build();

            var actual = GraphSageHelper.initializeSingleLabelFeatures(graph, config, AllocationTracker.empty());

            var expected = HugeObjectArray.newArray(double[].class, 3, AllocationTracker.empty());
            expected.setAll(i -> new double[] {5.0, 2.0, 7.0});

            assertEquals(expected.size(), actual.size());
            for (int i = 0; i < actual.size(); i++) {
                assertThat(actual.get(i)).containsExactlyInAnyOrder(expected.get(i));
            }
        }
    }
}
