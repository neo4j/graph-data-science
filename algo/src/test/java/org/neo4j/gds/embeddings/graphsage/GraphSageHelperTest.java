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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.junit.jupiter.api.Assertions.assertThrows;

@GdlExtension
class GraphSageHelperTest {

    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    private Graph graph;

    @ParameterizedTest(name = "{0}")
    @MethodSource({"parameters"})
    void shouldInitializeFeaturesCorrectly(String name, GraphSageTrainConfig config, HugeObjectArray<double[]> expected) {
        var actual = GraphSageHelper.initializeFeatures(graph, config, AllocationTracker.empty());

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
            .nodePropertyNames(Set.of("dummyProp"))
            .projectedFeatureSize(42)
            .build();
        var exception = assertThrows(IllegalArgumentException.class, () ->
            GraphSageHelper.initializeFeatures(graph, config, AllocationTracker.empty())
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
                    .nodePropertyNames(Set.of("dummyProp"))
                    .build(),
                    singleLabelProperties
            ), Arguments.of(
                "multi label",
                ImmutableGraphSageTrainConfig.builder()
                    .modelName("foo")
                    .nodePropertyNames(Set.of("numEmployees", "rating", "numIngredients", "numPurchases"))
                    .projectedFeatureSize(5)
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
                .nodePropertyNames(Set.of("prop"))
                .build();

            assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> GraphSageHelper.initializeFeatures(
                    graph,
                    graphSageTrainConfig,
                    AllocationTracker.empty()))
                .withMessageContaining(
                    formatWithLocale("Missing node property for property key `prop` on node with id `%s`.", idFunction.of("b"))
                );
        }
    }
}
