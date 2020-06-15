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
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;

@GdlExtension
class BetweennessCentralityTest {

    private static final AllocationTracker TRACKER = AllocationTracker.EMPTY;

    private static final BetweennessCentralityStreamConfig DEFAULT_CONFIG = BetweennessCentralityStreamConfig.of(
        "",
        Optional.empty(),
        Optional.empty(),
        CypherMapWrapper.empty()
    );

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (d)-[:REL]->(e)";

    @Inject
    private TestGraph graph;

    private static final double[] EXACT_CENTRALITIES = {0.0, 3.0, 4.0, 3.0, 0.0};
    private static final double[] EMPTY_CENTRALITIES = {0.0, 0.0, 0.0, 0.0, 0.0};

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testForceCompleteSampling(int concurrency) {
        var bc = new BetweennessCentrality(graph, new SelectionStrategy.All(), Pools.DEFAULT, concurrency, TRACKER);
        assertResult(bc.compute(), EXACT_CENTRALITIES);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testForceEmptySampling(int concurrency) {
        var bc = new BetweennessCentrality(graph, new SelectionStrategy.RandomDegree(0), Pools.DEFAULT, concurrency, TRACKER);
        assertResult(bc.compute(), EMPTY_CENTRALITIES);
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 6_000_368L, 6_000_368L),
            Arguments.of(4, 21_601_160L, 21_601_160L),
            Arguments.of(42, 219_211_192L, 219_211_192L)
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

    private void assertResult(HugeAtomicDoubleArray result, double[] centralities) {
        assertEquals(5, centralities.length, "Expected 5 centrality values");
        assertEquals(centralities[0], result.get((int) graph.toOriginalNodeId("a")));
        assertEquals(centralities[1], result.get((int) graph.toOriginalNodeId("b")));
        assertEquals(centralities[2], result.get((int) graph.toOriginalNodeId("c")));
        assertEquals(centralities[3], result.get((int) graph.toOriginalNodeId("d")));
        assertEquals(centralities[4], result.get((int) graph.toOriginalNodeId("e")));
    }
}
