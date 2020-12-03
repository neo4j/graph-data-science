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
package org.neo4j.graphalgo.beta.paths.dijkstra;

import com.carrotsearch.hppc.predicates.LongLongPredicate;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipProperties;
import org.neo4j.graphalgo.beta.paths.ImmutablePathResult;
import org.neo4j.graphalgo.beta.paths.PathResult;
import org.neo4j.graphalgo.beta.paths.dijkstra.config.ImmutableAllShortestPathsDijkstraStreamConfig;
import org.neo4j.graphalgo.beta.paths.dijkstra.config.ImmutableShortestPathDijkstraStreamConfig;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
final class DijkstraTest {

    @GdlGraph
    private static final String DUMMY = "()";

    static ImmutableShortestPathDijkstraStreamConfig.Builder defaultSourceTargetConfigBuilder() {
        return ImmutableShortestPathDijkstraStreamConfig.builder()
            .path(true)
            .concurrency(1);
    }

    static ImmutableAllShortestPathsDijkstraStreamConfig.Builder defaultSingleSourceConfigBuilder() {
        return ImmutableAllShortestPathsDijkstraStreamConfig.builder()
            .path(true)
            .concurrency(1);
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1_000, 32_736L),
            Arguments.of(1_000_000, 32_250_480L),
            Arguments.of(1_000_000_000, 32_254_883_392L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int nodeCount, long expectedBytes) {
        TestSupport.assertMemoryEstimation(
            Dijkstra::memoryEstimation,
            nodeCount,
            1,
            expectedBytes,
            expectedBytes
        );
    }

    @Nested
    @TestInstance(value = TestInstance.Lifecycle.PER_CLASS)
    class Graph1 {

        // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label)" +
            ", (b:Label)" +
            ", (c:Label)" +
            ", (d:Label)" +
            ", (e:Label)" +
            ", (f:Label)" +

            ", (a)-[:TYPE {cost: 4}]->(b)" +
            ", (a)-[:TYPE {cost: 2}]->(c)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (b)-[:TYPE {cost: 10}]->(d)" +
            ", (c)-[:TYPE {cost: 3}]->(e)" +
            ", (d)-[:TYPE {cost: 11}]->(f)" +
            ", (e)-[:TYPE {cost: 4}]->(d)";

        @Inject
        private Graph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void nonExisting() {
            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(idFunction.of("f"))
                .targetNode(idFunction.of("a"))
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .paths()
                .findFirst()
                .get();

            assertEquals(PathResult.EMPTY, path);
        }

        @Test
        void sourceTarget() {
            var expected = expected(graph, idFunction, 0, "a", "c", "e", "d", "f");

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(idFunction.of("a"))
                .targetNode(idFunction.of("f"))
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .paths()
                .findFirst()
                .get();

            assertEquals(expected, path);
        }

        @ParameterizedTest
        @MethodSource("predicatesAndPaths")
        void sourceTargetWithRelationshipFilter(LongLongPredicate graphFilter, List<String> expectedPath) {
            var expected = expected(graph, idFunction, 0, expectedPath.toArray(String[]::new));

            var sourceNode = idFunction.of(expectedPath.get(0));
            var targetNode = idFunction.of(expectedPath.get(expectedPath.size() - 1));

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(sourceNode)
                .targetNode(targetNode)
                .build();

            var dijkstra = Dijkstra
                .sourceTarget(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .withRelationshipFilter(graphFilter);
            var paths = dijkstra
                .compute()
                .paths()
                .findFirst()
                .get();

            assertEquals(expected, paths);
        }

        Stream<Arguments> predicatesAndPaths() {
            return Stream.of(
                Arguments.of((LongLongPredicate)(source, target) -> source != idFunction.of("c"), List.of("a", "b", "d", "f")),
                Arguments.of(
                    (LongLongPredicate)(source, target) -> !((source == idFunction.of("a") && target == idFunction.of("c")) ||
                                                           (source == idFunction.of("b") && target == idFunction.of("d"))),
                    List.of("a", "b", "c", "e", "d", "f"))
            );
        }

        @Test
        void singleSource() {
            var expected = Set.of(
                expected(graph, idFunction, 0, "a"),
                expected(graph, idFunction, 1, "a", "c"),
                expected(graph, idFunction, 2, "a", "b"),
                expected(graph, idFunction, 3, "a", "c", "e"),
                expected(graph, idFunction, 4, "a", "c", "e", "d"),
                expected(graph, idFunction, 5, "a", "c", "e", "d", "f")
            );

            var sourceNode = idFunction.of("a");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }

        @Test
        void singleSourceFromDisconnectedNode() {
            var expected = Set.of(
                expected(graph, idFunction, 0, "c"),
                expected(graph, idFunction, 1, "c", "e"),
                expected(graph, idFunction, 2, "c", "e", "d"),
                expected(graph, idFunction, 3, "c", "e", "d", "f")
            );

            var sourceNode = idFunction.of("c");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }

        @Test
        void shouldLogProgress() {
            var testLogger = new TestProgressLogger(graph.relationshipCount(), "Dijkstra", 1);

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(idFunction.of("a"))
                .targetNode(idFunction.of("f"))
                .build();

            var ignored = Dijkstra
                .sourceTarget(graph, config, testLogger, AllocationTracker.empty())
                .compute()
                .pathSet();

            List<AtomicLong> progresses = testLogger.getProgresses();
            assertEquals(1, progresses.size());
            assertEquals(graph.relationshipCount(), progresses.get(0).get());

            assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Start"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, "Dijkstra 28%"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, "Dijkstra 42%"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, "Dijkstra 71%"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, "Dijkstra 85%"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, "Dijkstra 100%"));
            assertTrue(testLogger.containsMessage(TestLog.INFO, ":: Finished"));

            // no duplicate entries in progress logger
            var logMessages = testLogger.getMessages(TestLog.INFO);
            assertEquals(Set.copyOf(logMessages).size(), logMessages.size());
        }
    }

    @Nested
    class Graph2 {

        // https://www.cise.ufl.edu/~sahni/cop3530/slides/lec326.pdf without relationship id 14
        @GdlGraph
        private static final String DB_CYPHER2 =
            "CREATE" +
            "  (n1:Label)" +
            ", (n2:Label)" +
            ", (n3:Label)" +
            ", (n4:Label)" +
            ", (n5:Label)" +
            ", (n6:Label)" +
            ", (n7:Label)" +

            ", (n1)-[:TYPE {cost: 6}]->(n2)" +
            ", (n1)-[:TYPE {cost: 2}]->(n3)" +
            ", (n1)-[:TYPE {cost: 16}]->(n4)" +
            ", (n2)-[:TYPE {cost: 4}]->(n5)" +
            ", (n2)-[:TYPE {cost: 5}]->(n4)" +
            ", (n3)-[:TYPE {cost: 7}]->(n2)" +
            ", (n3)-[:TYPE {cost: 3}]->(n5)" +
            ", (n3)-[:TYPE {cost: 8}]->(n6)" +
            ", (n4)-[:TYPE {cost: 7}]->(n3)" +
            ", (n5)-[:TYPE {cost: 4}]->(n4)" +
            ", (n5)-[:TYPE {cost: 10}]->(n7)" +
            ", (n6)-[:TYPE {cost: 1}]->(n7)";

        @Inject
        private Graph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void sourceTarget() {
            var expected = expected(graph, idFunction, 0, "n1", "n3", "n6", "n7");

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(idFunction.of("n1"))
                .targetNode(idFunction.of("n7"))
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .paths()
                .findFirst()
                .get();

            assertEquals(expected, path);
        }

        @Test
        void singleSource() {
            var expected = Set.of(
                expected(graph, idFunction, 0, "n1"),
                expected(graph, idFunction, 1, "n1", "n3"),
                expected(graph, idFunction, 2, "n1", "n3", "n5"),
                expected(graph, idFunction, 3, "n1", "n2"),
                expected(graph, idFunction, 4, "n1", "n3", "n5", "n4"),
                expected(graph, idFunction, 5, "n1", "n3", "n6"),
                expected(graph, idFunction, 6, "n1", "n3", "n6", "n7")
            );

            var sourceNode = idFunction.of("n1");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, ProgressLogger.NULL_LOGGER, AllocationTracker.empty())
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }
    }

    private PathResult expected(RelationshipProperties graph, IdFunction idFunction, long index, String... nodes) {
        var builder = ImmutablePathResult.builder()
            .index(index)
            .sourceNode(idFunction.of(nodes[0]))
            .targetNode(idFunction.of(nodes[nodes.length - 1]));

        var nodeIds = new ArrayList<Long>(nodes.length);
        var costs = new ArrayList<Double>(nodes.length);

        var cost = 0.0;
        var prevNode = -1L;

        for (int i = 0; i < nodes.length; i++) {
            var currentNode = idFunction.of(nodes[i]);
            if (i > 0) {
                cost += graph.relationshipProperty(prevNode, currentNode);
            }
            prevNode = currentNode;
            nodeIds.add(currentNode);
            costs.add(cost);
        }

        return builder
            .totalCost(costs.get(costs.size() - 1))
            .costs(costs)
            .nodeIds(nodeIds)
            .build();
    }
}
