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
package org.neo4j.gds.paths.bellmanford;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.paths.PathTestUtil.expected;

final class BellmanFordCopiedDeltaSteppingTests {

    // concurrency X id-supplier
    static Stream<Arguments> testParameters() {
        return TestSupport.crossArguments(
            () -> TestSupport.crossArguments(
                () -> IntStream.of(1, 4).mapToObj(Arguments::of)
            ),
            () -> Stream.of(0L, 42L).map(Arguments::of)
        );
    }

    @GdlExtension
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

            ", (a)-[:TYPE {cost:  4}]->(b)" +
            ", (a)-[:TYPE {cost:  2}]->(c)" +
            ", (b)-[:TYPE {cost:  5}]->(c)" +
            ", (b)-[:TYPE {cost: 10}]->(d)" +
            ", (c)-[:TYPE {cost:  3}]->(e)" +
            ", (d)-[:TYPE {cost: 11}]->(f)" +
            ", (e)-[:TYPE {cost:  4}]->(d)";

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.bellmanford.BellmanFordCopiedDeltaSteppingTests#testParameters")
        void singleSource(int concurrency, long idOffset) {
            var graph = TestSupport.fromGdl(DB_CYPHER, idOffset);

            IdFunction idFunction = (String variable) -> graph.toMappedNodeId(graph.toOriginalNodeId(variable));

            var expected = Set.of(
                expected(idFunction, 0, new double[]{0.0}, "a"),
                expected(idFunction, 1, new double[]{0.0, 4.0}, "a", "b"),
                expected(idFunction, 2, new double[]{0.0, 2.0}, "a", "c"),
                expected(idFunction, 3, new double[]{0.0, 2.0, 5.0}, "a", "c", "e"),
                expected(idFunction, 4, new double[]{0.0, 2.0, 5.0, 9.0}, "a", "c", "e", "d"),
                expected(idFunction, 5, new double[]{0.0, 2.0, 5.0, 9.0, 20.0}, "a", "c", "e", "d", "f")
            );

            var sourceNode = graph.toMappedNodeId("a");
            var paths = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, sourceNode, true, true, concurrency)
                .compute()
                .shortestPaths()
                .pathSet();
            assertEquals(expected, paths);
        }

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.bellmanford.BellmanFordCopiedDeltaSteppingTests#testParameters")
        void singleSourceFromDisconnectedNode(int concurrency, long idOffset) {
            var graph = TestSupport.fromGdl(DB_CYPHER, idOffset);

            IdFunction idFunction = (String variable) -> graph.toMappedNodeId(graph.toOriginalNodeId(variable));

            var expected = Set.of(
                expected(idFunction, 0, new double[]{0.0}, "c"),
                expected(idFunction, 1, new double[]{0.0, 3.0}, "c", "e"),
                expected(idFunction, 2, new double[]{0.0, 3.0, 7.0}, "c", "e", "d"),
                expected(idFunction, 3, new double[]{0.0, 3.0, 7.0, 18.0}, "c", "e", "d", "f")
            );

            var sourceNode = graph.toMappedNodeId("c");
            var paths = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, sourceNode, true, true, concurrency)
                .compute()
                .shortestPaths()
                .pathSet();
            assertEquals(expected, paths);
        }
    }

    @Nested
    class Graph2 {

        // https://www.cise.ufl.edu/~sahni/cop3530/slides/lec326.pdf without relationship id 14
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

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.bellmanford.BellmanFordCopiedDeltaSteppingTests#testParameters")
        void singleSource(int concurrency, long idOffset) {
            var graph = TestSupport.fromGdl(DB_CYPHER2, idOffset);

            IdFunction idFunction = (String variable) -> graph.toMappedNodeId(graph.toOriginalNodeId(variable));

            var expected = Set.of(
                expected(idFunction, 0, new double[]{0.0}, "n1"),
                expected(idFunction, 1, new double[]{0.0, 2.0}, "n1", "n3"),
                expected(idFunction, 2, new double[]{0.0, 2.0, 5.0}, "n1", "n3", "n5"),
                expected(idFunction, 3, new double[]{0.0, 6.0}, "n1", "n2"),
                expected(idFunction, 4, new double[]{0.0, 2.0, 5.0, 9.0}, "n1", "n3", "n5", "n4"),
                expected(idFunction, 5, new double[]{0.0, 2.0, 10.0}, "n1", "n3", "n6"),
                expected(idFunction, 6, new double[]{0.0, 2.0, 10.0, 11.0}, "n1", "n3", "n6", "n7")
            );

            var sourceNode = graph.toMappedNodeId("n1");
            var paths = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, sourceNode, true, true, concurrency)
                .compute()
                .shortestPaths()
                .pathSet();
            assertEquals(expected, paths);
        }
    }

    @GdlExtension
    @Nested
    @TestInstance(value = TestInstance.Lifecycle.PER_CLASS)
    class Graph3 {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:A)" +
            ", (b:B)" +
            ", (c:C)" +
            ", (d:D)" +
            ", (e:E)" +
            ", (f:F)" +

            ", (a)-[:TYPE]->(b)" +
            ", (a)-[:TYPE]->(c)" +
            ", (b)-[:TYPE]->(c)" +
            ", (b)-[:TYPE]->(d)" +
            ", (c)-[:TYPE]->(e)" +
            ", (d)-[:TYPE]->(f)" +
            ", (e)-[:TYPE]->(d)";

        @Inject
        Graph graph;

        @Inject
        IdFunction idFunction;

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.bellmanford.BellmanFordCopiedDeltaSteppingTests#testParameters")
        void singleSource(int concurrency) {
            var expected = Set.of(
                expected(idFunction, 0, new double[]{0.0}, "a"),
                expected(idFunction, 1, new double[]{0.0, 1.0}, "a", "b"),
                expected(idFunction, 2, new double[]{0.0, 1.0}, "a", "c"),
                expected(idFunction, 2, new double[]{0.0, 1.0, 2.0}, "a", "b", "d"),
                expected(idFunction, 3, new double[]{0.0, 1.0, 2.0}, "a", "c", "e"),
                expected(idFunction, 5, new double[]{0.0, 1.0, 2.0, 3.0}, "a", "b", "d", "f")
            );
            var sourceNode = graph.toOriginalNodeId(idFunction.of("a"));

            var paths = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, sourceNode, true, true, concurrency)
                .compute()
                .shortestPaths()
                .pathSet();
            assertEquals(expected, paths);
        }
    }

    private BellmanFordCopiedDeltaSteppingTests() {}
}
