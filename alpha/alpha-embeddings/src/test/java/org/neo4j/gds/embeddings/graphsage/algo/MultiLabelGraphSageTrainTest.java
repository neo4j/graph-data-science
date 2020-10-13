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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.stream.Stream;

@GdlExtension
class MultiLabelGraphSageTrainTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (r1:Restaurant {numEmployees: 5.0, rating: 2.0})" +
        ", (d1:Dish {numIngredients: 3.0, rating: 5.0})" +
        ", (c1:Customer {numPurchases: 15.0}) " +
        ", (r1)-[:SERVES]->(d1)" +
        ", (c1)-[:ORDERED {rating: 4.0}]->(d1)";

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRunWithOrWithoutDegreeAsProperty(boolean degreeAsProperty) {
        var config = ImmutableMultiLabelGraphSageTrainConfig.builder()
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(64)
            .modelName("foo")
            .degreeAsProperty(degreeAsProperty)
            .build();

        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            AllocationTracker.empty(),
            new TestLog()
        );
        // should not fail
        multiLabelGraphSageTrain.compute();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("featureSizes")
    void shouldRunWithDifferentProjectedFeatureSizes(String name, MultiLabelGraphSageTrainConfig config) {
        var multiLabelGraphSageTrain = new MultiLabelGraphSageTrain(
            graph,
            config,
            AllocationTracker.empty(),
            new TestLog()
        );
        // should not fail
        multiLabelGraphSageTrain.compute();
    }

    private static Stream<Arguments> featureSizes() {
        var builder = ImmutableMultiLabelGraphSageTrainConfig.builder()
            .nodePropertyNames(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(64)
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
