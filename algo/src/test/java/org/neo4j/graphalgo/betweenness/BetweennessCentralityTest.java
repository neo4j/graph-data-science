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
package org.neo4j.graphalgo.betweenness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class BetweennessCentralityTest {

    private static final AllocationTracker TRACKER = AllocationTracker.EMPTY;

    private static final BetweennessCentralityStreamConfig DEFAULT_CONFIG = BetweennessCentralityStreamConfig.of(
        "",
        Optional.empty(),
        Optional.empty(),
        CypherMapWrapper.empty()
    );

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

    static Stream<Arguments> testArguments() {
        return crossArguments(() -> Stream.of(1, 4).map(Arguments::of), BetweennessCentralityTest::expectedResults);
    }

    static Stream<Arguments> expectedResults() {
        return Stream.of(
            Arguments.of(fromGdl(LINE), 5, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 3.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE), 2, Map.of("a", 0.0, "b", 3.0, "c", 4.0, "d", 2.0, "e", 0.0)),
            Arguments.of(fromGdl(LINE), 0, Map.of("a", 0.0, "b", 0.0, "c", 0.0, "d", 0.0, "e", 0.0)),
            Arguments.of(fromGdl(CYCLE), 3, Map.of("a", 1.0, "b", 1.0, "c", 1.0))
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.betweenness.BetweennessCentralityTest#testArguments")
    void sampling(int concurrency, TestGraph graph, int samplingSize, Map<String, Double> expectedResult) {
        HugeAtomicDoubleArray actualResult = new BetweennessCentrality(
            graph,
            new SelectionStrategy.RandomDegree(samplingSize, Optional.of(42L)),
            Pools.DEFAULT,
            concurrency,
            TRACKER
        ).compute();

        assertEquals(expectedResult.size(), actualResult.size());
        expectedResult.forEach((variable, expectedCentrality) ->
            assertEquals(expectedCentrality, actualResult.get(graph.toMappedNodeId(variable)))
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void noSampling(int concurrency) {
        TestGraph graph = fromGdl(LINE);
        var actualResult = new BetweennessCentrality(graph, SelectionStrategy.ALL, Pools.DEFAULT, concurrency, TRACKER).compute();
        assertEquals(5, actualResult.size(), "Expected 5 centrality values");
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("a")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("b")));
        assertEquals(4.0, actualResult.get((int) graph.toMappedNodeId("c")));
        assertEquals(3.0, actualResult.get((int) graph.toMappedNodeId("d")));
        assertEquals(0.0, actualResult.get((int) graph.toMappedNodeId("e")));
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 6_000_360L, 6_000_360L),
            Arguments.of(4, 21_601_152L, 21_601_152L),
            Arguments.of(42, 219_211_184L, 219_211_184L)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.betweenness.BetweennessCentralityTest#expectedMemoryEstimation")
    void testMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        assertMemoryEstimation(
            () -> new BetweennessCentralityFactory<>().memoryEstimation(DEFAULT_CONFIG),
            100_000L,
            concurrency,
            expectedMinBytes,
            expectedMaxBytes
        );
    }
}
