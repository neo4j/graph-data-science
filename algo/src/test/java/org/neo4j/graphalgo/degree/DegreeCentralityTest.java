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
package org.neo4j.graphalgo.degree;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class DegreeCentralityTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1)" +
        ", (b:Label1)" +
        ", (c:Label1)" +
        ", (d:Label1)" +
        ", (e:Label1)" +
        ", (f:Label1)" +
        ", (g:Label1)" +
        ", (h:Label1)" +
        ", (i:Label1)" +
        ", (j:Label1)" +

        ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +

        ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

        ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

        ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 2.0}]->(e)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @ParameterizedTest
    @MethodSource("degreeCentralityParameters")
    void shouldComputeCorrectResults(boolean weighted, Map<String, Double> expected, boolean cacheDegrees, int concurrency) {
        var configBuilder = ImmutableDegreeCentralityConfig.builder();
        if (weighted) {
            configBuilder.relationshipWeightProperty("weight");
        }
        if (cacheDegrees) {
            configBuilder.cacheDegrees(true);
        }
        configBuilder.concurrency(concurrency);
        var config = configBuilder.build();

        var degreeCentrality = new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            config,
            ProgressLogger.NULL_LOGGER,
            AllocationTracker.empty()
        );

        var degreeFunction = degreeCentrality.compute();
        expected.forEach((variable, expectedDegree) -> {
            long nodeId = graph.toMappedNodeId(idFunction.of(variable));
            assertEquals(degreeFunction.get(nodeId), expectedDegree, 1E-6);
        });
    }

    static Stream<Arguments> degreeCentralityParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of(
                    true,
                    Map.of("a", 0.0D, "b", 2.0D, "c", 2.0D, "d", 4.0D, "e", 6.0D, "f", 4.0D),
                    true
                ),
                Arguments.of(
                    true,
                    Map.of("a", 0.0D, "b", 2.0D, "c", 2.0D, "d", 4.0D, "e", 6.0D, "f", 4.0D),
                    false
                ),
                Arguments.of(
                    false,
                    Map.of("a", 0.0D, "b", 1.0D, "c", 1.0D, "d", 2.0D, "e", 3.0D, "f", 2.0D),
                    false
                )
            ),
            () -> Stream.of(Arguments.of(1), Arguments.of(4))
        );
    }
}
