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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.TestGraph;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;

class BetweennessCentralityTest {

    private static final BetweennessCentralityStreamConfig DEFAULT_CONFIG = BetweennessCentralityStreamConfig.of(CypherMapWrapper.empty());
    private static final BetweennessCentralityStreamConfig WEIGHTED_CONFIG =
        BetweennessCentralityStreamConfig.of(CypherMapWrapper.create(Map.of("relationshipWeightProperty", "foo")));

    private static final String DIAMOND =
        "CREATE " +
        "  (a1)-[:REL]->(b)" +
        ", (a2)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)" +
        ", (c)-[:REL]->(e)" +
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)";

    private static final String LINE =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    private static final String CYCLE =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)";

    private static final String CLIQUE_5 =
        "CREATE" +
        "  (a)-[:REL]->(b)" +
        "  (a)-[:REL]->(c)" +
        "  (a)-[:REL]->(d)" +
        "  (a)-[:REL]->(e)" +
        ", (b)-[:REL]->(c)" +
        ", (b)-[:REL]->(d)" +
        ", (b)-[:REL]->(e)" +
        ", (c)-[:REL]->(d)" +
        ", (c)-[:REL]->(e)" +
        ", (d)-[:REL]->(e)";

    private static final String DISCONNECTED_CYCLES =
        "CREATE" +
        // Cycle 1
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        // Cycle 2
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)" +
        ", (f)-[:REL]->(d)";

    private static final String CONNECTED_CYCLES =
        "CREATE" +
        // Cycle 1
        "  (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        // Cycle 2
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(f)" +
        ", (f)-[:REL]->(d)" +
        // Connection
        ", (a)-[:REL]->(d)" +
        ", (d)-[:REL]->(a)";

    static Stream<Arguments> testArguments() {
        return crossArguments(() -> Stream.of(1, 4).map(Arguments::of), BetweennessCentralityTest::expectedResults);
    }

    static Stream<Arguments> expectedResults() {
        return Stream.of(
            Arguments.of(fromGdl(LINE, "line"), 5, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 3.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE, "line"), 2, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 2.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE, "line"), 0, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CYCLE, "cycle"), 3, Map.of("a", 1.0, "b", 1.0, "c", 1.0)),
            Arguments.of(fromGdl(CLIQUE_5, "clique_5"), 5, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CLIQUE_5, UNDIRECTED, "undirected_clique_5"), 5, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CLIQUE_5, UNDIRECTED,"undirected_clique_5"), 3, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(DISCONNECTED_CYCLES, "disconnected_cycles"), 6, Map.of("a", 1.0, "b", 1.0, "c", 1.0, "d", 1.0, "e", 1.0, "f", 1.0)),
            Arguments.of(fromGdl(CONNECTED_CYCLES, "connected_cycles"), 6, Map.of("a", 13.0, "b", 4.0, "c", 4.0, "d", 13.0, "e", 4.0, "f", 4.0)),
            Arguments.of(fromGdl(CONNECTED_CYCLES, "connected_cycles"), 2, Map.of("a", 4.0, "b", 1.0, "c", 0.0, "d", 4.0, "e", 2.0, "f", 0.0)),
            Arguments.of(fromGdl(DIAMOND, "diamond"), 7, Map.of("a1", 0.0, "a2", 0.0, "b", 8.0, "c", 3.0, "d", 3.0, "e", 5.0, "f", 0.0)),
            Arguments.of(fromGdl(DIAMOND, UNDIRECTED, "undirected_diamond"), 7, Map.of("a1", 0.0, "a2", 0.0, "b", 9.5, "c", 3.0, "d", 3.0, "e", 5.5, "f", 0.0))
        );
    }

    @ParameterizedTest(name = "graph={1}, concurrency={0}, samplingSize={2}")
    @MethodSource("org.neo4j.gds.betweenness.BetweennessCentralityTest#testArguments")
    void sampling(int concurrency, TestGraph graph, int samplingSize, Map<String, Double> expectedResult) {
        HugeAtomicDoubleArray actualResult = new BetweennessCentrality(
            graph,
            new SelectionStrategy.RandomDegree(samplingSize, Optional.of(42L)),
            false,
            Pools.DEFAULT,
            concurrency,
            ProgressTracker.NULL_TRACKER
        ).compute();

        assertEquals(expectedResult.size(), actualResult.size());
        expectedResult.forEach((variable, expectedCentrality) ->
            assertEquals(expectedCentrality, actualResult.get(graph.toMappedNodeId(variable)), variable)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void noSampling(int concurrency) {
        TestGraph graph = fromGdl(LINE);
        var actualResult = new BetweennessCentrality(
            graph,
            SelectionStrategy.ALL,
            false,
            Pools.DEFAULT,
            concurrency,
            ProgressTracker.NULL_TRACKER
        ).compute();
        assertEquals(5, actualResult.size(), "Expected 5 centrality values");
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("a")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("b")));
        assertEquals(4.0, actualResult.get((int) graph.toMappedNodeId("c")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("d")));
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("e")));
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 6_000_376L, 6_000_376L),
            Arguments.of(4, 21_601_192L, 21_601_192L),
            Arguments.of(42, 219_211_528L, 219_211_528L)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.betweenness.BetweennessCentralityTest#expectedMemoryEstimation")
    void testMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        assertMemoryEstimation(
            () -> new BetweennessCentralityFactory<>().memoryEstimation(DEFAULT_CONFIG),
            100_000L,
            concurrency,
            MemoryRange.of(expectedMinBytes, expectedMaxBytes)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedWeightedMemoryEstimation")
    void testMemoryEstimationWeighted(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        assertMemoryEstimation(
            () -> new BetweennessCentralityFactory<>().memoryEstimation(WEIGHTED_CONFIG),
            100_000L,
            concurrency,
            MemoryRange.of(expectedMinBytes, expectedMaxBytes)
        );
    }

    static Stream<Arguments> expectedWeightedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 6_425_504L, 6_425_504L),
            Arguments.of(4, 23_301_704L, 23_301_704L),
            Arguments.of(42, 237_066_904L, 237_066_904L)
        );
    }

}
