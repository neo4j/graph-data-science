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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
final class PageRankTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = Orientation.NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    private static final String GRAPH =
        "CREATE" +
        "  (a:Label)" +
        ", (b:Label)" +
        ", (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (g:Label)" +
        ", (h:Label)" +
        ", (i:Label)" +
        ", (j:Label)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(b)" +
        ", (d)-[:TYPE]->(a)" +
        ", (d)-[:TYPE]->(b)" +
        ", (e)-[:TYPE]->(b)" +
        ", (e)-[:TYPE]->(d)" +
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(b)" +
        ", (f)-[:TYPE]->(e)";

    @Inject
    private TestGraph naturalGraph;

    @Inject
    private TestGraph reverseGraph;

    private static final PageRankBaseConfig DEFAULT_CONFIG = defaultConfigBuilder().build();

    static ImmutablePageRankStreamConfig.Builder defaultConfigBuilder() {
        return ImmutablePageRankStreamConfig.builder()
            .maxIterations(40);
    }

    @Test
    void testOnOutgoingRelationships() {
        var expected = Map.of(
            "a", 0.243007,
            "b", 1.9183995,
            "c", 1.7806315,
            "d", 0.21885,
            "e", 0.243007,
            "f", 0.21885,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        assertResult(this.naturalGraph, PageRankAlgorithmType.NON_WEIGHTED, expected);
    }

    @Test
    void testOnIncomingRelationships() {
        var expected = Map.of(
            "a", 0.15,
            "b", 0.3386727,
            "c", 0.2219679,
            "d", 0.3494679,
            "e", 2.5463981,
            "f", 2.3858317,
            "g", 0.15,
            "h", 0.15,
            "i", 0.15,
            "j", 0.15
        );

        assertResult(reverseGraph, PageRankAlgorithmType.NON_WEIGHTED, expected);
    }

    @Test
    void correctPartitionBoundariesForAllNodes() {
        assertThatCode(() -> {
            // explicitly list all source nodes to prevent the 'we got everything' optimization
            PageRankAlgorithmType.NON_WEIGHTED
                .create(
                    naturalGraph,
                    LongStream.range(0L, naturalGraph.nodeCount()),
                    defaultConfigBuilder().concurrency(1).build(),
                    null,
                    ProgressLogger.NULL_LOGGER,
                    AllocationTracker.empty()
                )
                .compute();
        }).doesNotThrowAnyException();
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 2000416L, 2000416L),
            Arguments.of(4, 3201304L, 3201304L),
            Arguments.of(42, 18451288L, 18451288L)
        );
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankTest#expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        var config = defaultConfigBuilder().build();
        var nodeCount = 100_000;
        var relationshipCount = nodeCount * 10;
        assertMemoryEstimation(
            () -> new PageRankFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            concurrency,
            expectedMinBytes,
            expectedMaxBytes
        );
    }

    @Test
    void shouldComputeMemoryEstimationFor10BElements() {
        var config = defaultConfigBuilder().build();
        var nodeCount = 10_000_000_000L;
        var relationshipCount = 10_000_000_000L;
        assertMemoryEstimation(
            () -> new PageRankFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            4,
            601295416504L,
            601295416504L
        );
    }

    @Test
    void shouldLogProgress() {
        var config = ImmutablePageRankStreamConfig.builder().build();

        var testLogger = new TestProgressLogger(
            naturalGraph.relationshipCount(),
            "PageRank",
            config.concurrency()
        );

        var pageRank = PageRankAlgorithmType.NON_WEIGHTED.create(
            naturalGraph,
            config,
            LongStream.empty(),
            testLogger,
            AllocationTracker.empty()
        );

        pageRank.compute();

        List<AtomicLong> progresses = testLogger.getProgresses();

        assertEquals(progresses.size(), pageRank.iterations());
        progresses.forEach(progress -> assertEquals(naturalGraph.relationshipCount(), progress.get()));

        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
        LongStream.range(1, pageRank.iterations() + 1).forEach(iteration -> {
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Iteration %d :: Start", iteration)));
            assertTrue(testLogger.containsMessage(TestLog.INFO, formatWithLocale("Iteration %d :: Start", iteration)));
        });
        assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));
    }

    static void assertResult(TestGraph graph, PageRankAlgorithm algorithmType, Map<String, Double> expected) {
        var rankResult = algorithmType
            .create(graph, DEFAULT_CONFIG, LongStream.empty(), ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
            .compute()
            .result();

        expected.forEach((node, expectedPageRank) -> {
            assertEquals(
                expected.get(node),
                rankResult.score(graph.toOriginalNodeId(node)),
                1e-2,
                "Node#" + node
            );
        });
    }
}
