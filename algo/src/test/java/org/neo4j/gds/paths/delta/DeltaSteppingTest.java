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
package org.neo4j.gds.paths.delta;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
final class DeltaSteppingTest {

    @GdlGraph
    private static final String DUMMY = "()";

    public static Stream<Arguments> deltaAndConcurrency() {
        return TestSupport.crossArguments(
            () -> DoubleStream.of(0.25, 0.5, 1, 2, 8).mapToObj(Arguments::of),
            () -> IntStream.of(1, 4).mapToObj(Arguments::of)
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

        @Inject
        private IdFunction idFunction;

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.delta.DeltaSteppingTest#deltaAndConcurrency")
        void singleSource(double delta, int concurrency) {
            var expected = Map.of(
                graph.toMappedNodeId("a"), 0.0,
                graph.toMappedNodeId("c"), 2.0,
                graph.toMappedNodeId("b"), 4.0,
                graph.toMappedNodeId("e"), 5.0,
                graph.toMappedNodeId("d"), 9.0,
                graph.toMappedNodeId("f"), 20.0
            );

            var sourceNode = idFunction.of("a");

            var actual = new DeltaStepping(
                graph,
                sourceNode,
                delta,
                concurrency,
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER,
                AllocationTracker.empty()
            ).compute();

            assertResultEquals(graph, expected, actual);
        }

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.delta.DeltaSteppingTest#deltaAndConcurrency")
        void singleSourceFromDisconnectedNode(double delta, int concurrency) {
            var expected = Map.of(
                graph.toMappedNodeId("c"), 0.0,
                graph.toMappedNodeId("e"), 3.0,
                graph.toMappedNodeId("d"), 7.0,
                graph.toMappedNodeId("f"), 18.0
            );

            var sourceNode = idFunction.of("c");

            var actual = new DeltaStepping(
                graph,
                sourceNode,
                delta,
                concurrency,
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER,
                AllocationTracker.empty()
            ).compute();

            assertResultEquals(graph, expected, actual);
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

        @Inject
        private IdFunction idFunction;

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.paths.delta.DeltaSteppingTest#deltaAndConcurrency")
        void singleSource(double delta, int concurrency) {
            var expected = Map.of(
                graph.toMappedNodeId("n1"), 0.0,
                graph.toMappedNodeId("n3"), 2.0,
                graph.toMappedNodeId("n5"), 5.0,
                graph.toMappedNodeId("n2"), 6.0,
                graph.toMappedNodeId("n4"), 9.0,
                graph.toMappedNodeId("n6"), 10.0,
                graph.toMappedNodeId("n7"), 11.0
            );

            var sourceNode = idFunction.of("n1");

            var actual = new DeltaStepping(
                graph,
                sourceNode,
                delta,
                concurrency,
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER,
                AllocationTracker.empty()
            ).compute();

            assertResultEquals(graph, expected, actual);
        }
    }

    private void assertResultEquals(
        Graph graph,
        Map<Long, ? extends Number> expected,
        DeltaStepping.DeltaSteppingResult actual
    ) {
        for (long i = 0; i < graph.nodeCount(); i++) {
            if (expected.containsKey(i)) {
                assertThat(actual.distance(i)).isEqualTo(expected.get(i));
            } else {
                assertThat(actual.distance(i)).isEqualTo(Double.MAX_VALUE);
            }
        }
    }
}
