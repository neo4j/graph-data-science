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
package org.neo4j.gds.paths.dijkstra;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.dijkstra.config.ImmutableAllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ImmutableShortestPathDijkstraStreamConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.paths.PathTestUtil.expected;

@GdlExtension
final class DijkstraTest {

    @GdlGraph
    private static final String DUMMY = "()";

    static ImmutableShortestPathDijkstraStreamConfig.Builder defaultSourceTargetConfigBuilder() {
        return ImmutableShortestPathDijkstraStreamConfig.builder()
            .concurrency(1);
    }

    static ImmutableAllShortestPathsDijkstraStreamConfig.Builder defaultSingleSourceConfigBuilder() {
        return ImmutableAllShortestPathsDijkstraStreamConfig.builder()
            .concurrency(1);
    }

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            // trackRelationships = false
            Arguments.of(1_000, false, 40_616),
            Arguments.of(1_000_000, false, 40_125_488L),
            Arguments.of(1_000_000_000, false, 40_131_104_128L),
            // trackRelationships = true
            Arguments.of(1_000, true, 56_832L),
            Arguments.of(1_000_000, true, 56_125_704, 56_125_704L),
            Arguments.of(1_000_000_000, true, 56_133_545_800L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int nodeCount, boolean trackRelationships, long expectedBytes) {
        TestSupport.assertMemoryEstimation(
            () -> Dijkstra.memoryEstimation(trackRelationships),
            nodeCount,
            1,
            MemoryRange.of(expectedBytes)
        );
    }

    @Nested
    @TestInstance(value = TestInstance.Lifecycle.PER_CLASS)
    class Graph1 {

        // https://en.wikipedia.org/wiki/Shortest_path_problem#/media/File:Shortest_path_with_direct_weights.svg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:A)" +
            ", (b:B)" +
            ", (c:C)" +
            ", (d:D)" +
            ", (e:E)" +
            ", (f:F)" +

            ", (a)-[:TYPE {cost: 4}]->(b)" +
            ", (a)-[:TYPE {cost: 2}]->(c)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (b)-[:TYPE {cost: 10}]->(d)" +
            ", (c)-[:TYPE {cost: 3}]->(e)" +
            ", (d)-[:TYPE {cost: 11}]->(f)" +
            ", (e)-[:TYPE {cost: 4}]->(d)";

        @Inject
        private TestGraph graph;

        @Test
        void nonExisting() {
            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("f"))
                .targetNode(graph.toOriginalNodeId("a"))
                .build();

            var paths = Dijkstra
                .sourceTarget(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .pathSet();

            assertTrue(paths.isEmpty());
        }

        @Test
        void sourceTarget() {
            var expected = expected(graph::toMappedNodeId, 0, new double[]{0.0, 2.0, 5.0, 9.0, 20.0}, "a", "c", "e", "d", "f");

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("a"))
                .targetNode(graph.toOriginalNodeId("f"))
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .findFirst()
                .get();

            assertEquals(expected, path);
        }

        @ParameterizedTest
        @MethodSource("predicatesAndPaths")
        void sourceTargetWithRelationshipFilter(Dijkstra.RelationshipFilter relationshipFilter, double[] expectedCosts, List<String> expectedPath) {
            var expected = expected(graph::toMappedNodeId, 0, expectedCosts, expectedPath.toArray(String[]::new));

            var sourceNode = graph.toOriginalNodeId(expectedPath.get(0));
            var targetNode = graph.toOriginalNodeId(expectedPath.get(expectedPath.size() - 1));

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(sourceNode)
                .targetNode(targetNode)
                .build();

            var dijkstra = Dijkstra
                .sourceTarget(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .withRelationshipFilter(relationshipFilter);
            var paths = dijkstra
                .compute()
                .findFirst()
                .get();

            assertEquals(expected, paths);
        }

        @Test
        void sourceTargetWithRelationshipIds() {
            var expected = ImmutablePathResult
                .builder()
                .from(expected(graph::toMappedNodeId, 0, new double[]{0.0, 2.0, 5.0, 9.0, 20.0}, "a", "c", "e", "d", "f"))
                .relationshipIds(1, 0, 0, 0)
                .build();

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("a"))
                .targetNode(graph.toOriginalNodeId("f"))
                .trackRelationships(true)
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .findFirst()
                .get();

            assertEquals(expected, path);
        }

        Stream<Arguments> predicatesAndPaths() {
            IdFunction idFunction = graph::toMappedNodeId;
            return Stream.of(
                Arguments.of((Dijkstra.RelationshipFilter) (source, target, relationshipId) ->
                    source != idFunction.of("c"), new double[]{0.0, 4.0, 14.0, 25.0}, List.of("a", "b", "d", "f")),
                Arguments.of(
                    (Dijkstra.RelationshipFilter) (source, target, relationshipId) ->
                        !((source == idFunction.of("a") && target == idFunction.of("c")) ||
                          (source == idFunction.of("b") && target == idFunction.of("d"))),
                    new double[]{0.0, 4.0, 9.0, 12.0, 16.0, 27.0},
                    List.of("a", "b", "c", "e", "d", "f")
                )
            );
        }

        @Test
        void singleSource() {
            var expected = Set.of(
                expected(graph::toMappedNodeId, 0, new double[]{0.0}, "a"),
                expected(graph::toMappedNodeId, 1, new double[]{0.0, 2.0}, "a", "c"),
                expected(graph::toMappedNodeId, 2, new double[]{0.0, 4.0}, "a", "b"),
                expected(graph::toMappedNodeId, 3, new double[]{0.0, 2.0, 5.0}, "a", "c", "e"),
                expected(graph::toMappedNodeId, 4, new double[]{0.0, 2.0, 5.0, 9.0}, "a", "c", "e", "d"),
                expected(graph::toMappedNodeId, 5, new double[]{0.0, 2.0, 5.0, 9.0, 20.0}, "a", "c", "e", "d", "f")
            );

            var sourceNode = graph.toOriginalNodeId("a");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }

        @Test
        void singleSourceFromDisconnectedNode() {
            IdFunction idFunction = graph::toMappedNodeId;

            var expected = Set.of(
                expected(idFunction, 0, new double[]{0.0}, "c"),
                expected(idFunction, 1, new double[]{0.0, 3.0}, "c", "e"),
                expected(idFunction, 2, new double[]{0.0, 3.0, 7.0}, "c", "e", "d"),
                expected(idFunction, 3, new double[]{0.0, 3.0, 7.0, 18.0}, "c", "e", "d", "f")
            );

            var sourceNode = graph.toOriginalNodeId("c");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }

        @Test
        void shouldLogProgress() {

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("a"))
                .targetNode(graph.toOriginalNodeId("f"))
                .build();

            var progressTask = new DijkstraFactory.SourceTargetDijkstraFactory<>().progressTask(graph, config);
            var testLog = Neo4jProxy.testLog();
            var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);

            Dijkstra.sourceTarget(graph, config, Optional.empty(), progressTracker)
                .compute()
                .pathSet();

            List<AtomicLong> progresses = progressTracker.getProgresses();
            assertEquals(1, progresses.size());
            assertEquals(graph.relationshipCount(), progresses.get(0).get());

            assertTrue(testLog.containsMessage(TestLog.INFO, ":: Start"));
            assertTrue(testLog.containsMessage(TestLog.INFO, "Dijkstra 28%"));
            assertTrue(testLog.containsMessage(TestLog.INFO, "Dijkstra 42%"));
            assertTrue(testLog.containsMessage(TestLog.INFO, "Dijkstra 71%"));
            assertTrue(testLog.containsMessage(TestLog.INFO, "Dijkstra 85%"));
            assertTrue(testLog.containsMessage(TestLog.INFO, "Dijkstra 100%"));
            assertTrue(testLog.containsMessage(TestLog.INFO, ":: Finished"));

            // no duplicate entries in progress logger
            var logMessages = testLog.getMessages(TestLog.INFO);
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
        private TestGraph graph;

        @Test
        void sourceTarget() {
            var expected = expected(graph::toMappedNodeId, 0, new double[]{0.0, 2.0, 10.0, 11.0}, "n1", "n3", "n6", "n7");

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("n1"))
                .targetNode(graph.toOriginalNodeId("n7"))
                .build();

            var path = Dijkstra
                .sourceTarget(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .findFirst()
                .get();

            assertEquals(expected, path);
        }

        @Test
        void singleSource() {
            IdFunction mappedId = graph::toMappedNodeId;

            var expected = Set.of(
                expected(mappedId, 0, new double[]{0.0}, "n1"),
                expected(mappedId, 1, new double[]{0.0, 2.0}, "n1", "n3"),
                expected(mappedId, 2, new double[]{0.0, 2.0, 5.0}, "n1", "n3", "n5"),
                expected(mappedId, 3, new double[]{0.0, 6.0}, "n1", "n2"),
                expected(mappedId, 4, new double[]{0.0, 2.0, 5.0, 9.0}, "n1", "n3", "n5", "n4"),
                expected(mappedId, 5, new double[]{0.0, 2.0, 10.0}, "n1", "n3", "n6"),
                expected(mappedId, 6, new double[]{0.0, 2.0, 10.0, 11.0}, "n1", "n3", "n6", "n7")
            );

            var sourceNode = graph.toOriginalNodeId("n1");

            var config = defaultSingleSourceConfigBuilder()
                .sourceNode(sourceNode)
                .build();

            var paths = Dijkstra.singleSource(graph, config, Optional.empty(), ProgressTracker.NULL_TRACKER)
                .compute()
                .pathSet();

            assertEquals(expected, paths);
        }
    }

    @Nested
    class Graph3 {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label { distance: 1.0 })" +
            ", (b:Label { distance: 42.0 })" + // avoid visiting that node
            ", (c:Label { distance: 1.0 })" +
            ", (d:Label { distance: 1.0 })" +
            ", (e:Label { distance: 1.0 })" +
            ", (f:Label { distance: 0.0 })" +

            ", (a)-[:TYPE {cost: 4}]->(b)" +
            ", (a)-[:TYPE {cost: 2}]->(c)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (b)-[:TYPE {cost: 10}]->(d)" +
            ", (c)-[:TYPE {cost: 3}]->(e)" +
            ", (d)-[:TYPE {cost: 11}]->(f)" +
            ", (e)-[:TYPE {cost: 4}]->(d)";

        @Inject
        TestGraph graph;

        @Test
        void sourceTargetWithHeuristic() {
            var expected = expected(graph::toMappedNodeId, 0, new double[]{0.0, 2.0, 5.0, 9.0, 20.0}, "a", "c", "e", "d", "f");

            var config = defaultSourceTargetConfigBuilder()
                .sourceNode(graph.toOriginalNodeId("a"))
                .targetNode(graph.toOriginalNodeId("f"))
                .build();

            var heapComparisons = new ArrayList<Long>();

            Dijkstra.HeuristicFunction heuristicFunction = (nodeId) -> {
                heapComparisons.add(nodeId);
                return graph.nodeProperties("distance").doubleValue(nodeId);
            };

            var path = Dijkstra
                .sourceTarget(graph, config, Optional.of(heuristicFunction), ProgressTracker.NULL_TRACKER)
                .compute()
                .findFirst()
                .get();

            assertEquals(List.of(2L, 1L, 4L, 1L, 3L, 1L, 5L, 1L), heapComparisons);
            assertEquals(expected, path);
        }
    }
}
