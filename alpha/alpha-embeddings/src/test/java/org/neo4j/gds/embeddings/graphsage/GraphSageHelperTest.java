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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableMultiLabelGraphSageTrainConfig;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class GraphSageHelperTest {

    @GdlGraph
    private static final String GDL = "CREATE" +
                                      "  (r1:Restaurant {dummyProperty: 5.0, numEmployees: 5.0, rating: 2.0})" +
                                      ", (d1:Dish       {dummyProperty: 5.0, numIngredients: 3.0, rating: 5.0})" +
                                      ", (c1:Customer   {dummyProperty: 5.0, numPurchases: 15.0}) " +
                                      ", (r1)-[:SERVES]->(d1)" +
                                      ", (c1)-[:ORDERED {rating: 4.0}]->(d1)";

    @Inject
    Graph graph;

    @ParameterizedTest(name = "{0}")
    @MethodSource({"parameters"})
    void shouldInitializeFeaturesCorrectly(String name, GraphSageTrainConfig config, HugeObjectArray<double[]> expected) {
        var actual = GraphSageHelper.initializeFeatures(graph, config, AllocationTracker.empty());

        assertEquals(expected.size(), actual.size());
        for(int i = 0; i < actual.size(); i++) {
            assertThat(actual.get(i)).containsExactlyInAnyOrder(expected.get(i));
        }
    }

    static Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                "single label",
                ImmutableGraphSageTrainConfig.builder()
                    .modelName("foo")
                    .nodePropertyNames(Set.of("dummyProperty"))
                    .build(),
                HugeObjectArray.of(
                    new double[]{5.0},
                    new double[]{5.0},
                    new double[]{5.0}
                )
            ), Arguments.of(
                "multi label",
                ImmutableMultiLabelGraphSageTrainConfig.builder()
                    .modelName("foo")
                    .nodePropertyNames(Set.of("numEmployees", "rating", "numIngredients", "numPurchases"))
                    .build(),
                HugeObjectArray.of(
                    new double[]{5.0, 2.0},
                    new double[]{3.0, 5.0},
                    new double[]{15.0}
                )
            )
        );
    }
}
