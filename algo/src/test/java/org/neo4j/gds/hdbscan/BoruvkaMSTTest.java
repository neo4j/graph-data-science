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
package org.neo4j.gds.hdbscan;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class BoruvkaMSTTest {

    @Nested
    class Case1 {

        @GdlGraph
        private static final String DATA =
            """
                CREATE
                    (a:Node {point: [1.0, 1.0]}),
                    (b:Node {point: [1.0, 5.0]}),
                    (c:Node {point: [1.0, 6.0]}),
                    (d:Node {point: [2.0, 2.0]}),
                    (e:Node {point: [8.0, 2.0]}),
                    (f:Node {point: [10.0, 1.0]})
                    (g:Node {point: [10.0, 2.0]})
                    (h:Node {point: [12.0, 3.0]})
                    (i:Node {point: [12.0, 21.0]})
                """;

        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @ValueSource(ints={1,4})
        void shouldReturnEuclideanMSTWithZeroCoreValues(int concurrency) {
            var nodePropertyValues = graph.nodeProperties("point");
            var kdTree = new KdTreeBuilder(graph,
                nodePropertyValues,
                1,
                1,
                ProgressTracker.NULL_TRACKER
            ).build();

            var dualTree =  BoruvkaMST.createWithZeroCores(
                nodePropertyValues,
                kdTree,
                graph.nodeCount(),
                new Concurrency(concurrency),
                ProgressTracker.NULL_TRACKER
            );

            var result = dualTree.compute();

            var expected = List.of(
                new Edge(graph.toMappedNodeId("g"), graph.toMappedNodeId("h"), 2.23606797749979),
                new Edge(graph.toMappedNodeId("i"), graph.toMappedNodeId("h"), 18.0),
                new Edge(graph.toMappedNodeId("a"), graph.toMappedNodeId("d"), 1.4142135623730951),
                new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"), 1.0),
                new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("g"), 2.0),
                new Edge(graph.toMappedNodeId("f"), graph.toMappedNodeId("g"), 1.0),
                new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("d"), 6.0),
                new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("d"), 3.1622776601683795)
            );

            assertThat(result.edges().toArray())
                .usingElementComparator(new UndirectedEdgeComparator())
                .containsExactlyInAnyOrderElementsOf(expected);

            assertThat(result.totalDistance())
                .isEqualTo(expected.stream()
                    .mapToDouble(Edge::distance)
                    .sum()
                );
        }

    }

    @Nested
    class Case2 {

        @GdlGraph
        private static final String DATA =
            """
                    CREATE
                        (a:Node { point: [2.0, 3.0]}),
                        (b:Node { point: [5.0, 4.0]}),
                        (c:Node { point: [9.0, 6.0]}),
                        (d:Node { point: [4.0, 7.0]}),
                        (e:Node { point: [8.0, 1.0]}),
                        (f:Node { point: [7.0, 2.0]})
                """;

        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @ValueSource(ints={1,4})
        void shouldReturnEuclideanMSTWithZeroCoreValues(int concurrency) {
            var nodePropertyValues = graph.nodeProperties("point");
            var kdTree = new KdTreeBuilder(
                graph,
                nodePropertyValues,
                1,
                1,
                ProgressTracker.NULL_TRACKER
            ).build();

            var dualTree =  BoruvkaMST.createWithZeroCores(
                nodePropertyValues,
                kdTree,
                graph.nodeCount(),
                new Concurrency(concurrency),
                ProgressTracker.NULL_TRACKER
            );

            var result = dualTree.compute();

            var expected = List.of(
                new Edge(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), 3.1622776601683795),
                new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("f"), 2.8284271247461903),
                new Edge(graph.toMappedNodeId("c"), graph.toMappedNodeId("f"), 4.47213595499958),
                new Edge(graph.toMappedNodeId("d"), graph.toMappedNodeId("b"), 3.1622776601683795),
                new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("f"), 1.4142135623730951)
            );

            assertThat(result.edges().toArray())
                .usingElementComparator(new UndirectedEdgeComparator())
                .containsExactlyInAnyOrderElementsOf(expected);

            assertThat(result.totalDistance())
                .isCloseTo(
                    expected.stream()
                        .mapToDouble(Edge::distance)
                        .sum(),
                    Offset.offset(1e-12)
                );
        }

    }

    @Nested
    class Case3 {

        // Example from https://github.com/mlpack/mlpack/blob/master/doc/tutorials/emst.md
        @GdlGraph
        private static final String DATA =
            """
                    CREATE
                        (a:Node { point: [0.0, 0.0] }),
                        (b:Node { point: [1.0, 1.0] }),
                        (c:Node { point: [3.0, 3.0] }),
                        (d:Node { point: [0.5, 0.0] }),
                        (e:Node { point: [1000.0, 0.0] }),
                        (f:Node { point: [1001.0, 0.0] })
                """;

        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @ValueSource(ints={1,4})
        void shouldReturnEuclideanMSTWithZeroCoreValues(int concurrency) {
            var nodePropertyValues = graph.nodeProperties("point");
            var kdTree = new KdTreeBuilder(graph,
                nodePropertyValues,
                1,
                1,
                ProgressTracker.NULL_TRACKER
            ).build();

            var dualTree =  BoruvkaMST.createWithZeroCores(
                nodePropertyValues,
                kdTree,
                graph.nodeCount(),
                new Concurrency(concurrency),
                ProgressTracker.NULL_TRACKER
            );

            var result = dualTree.compute();

            var expected = List.of(
                new Edge(graph.toMappedNodeId("a"), graph.toMappedNodeId("d"), 0.5),
                new Edge(graph.toMappedNodeId("e"), graph.toMappedNodeId("f"), 1.0),
                new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("d"), 1.118033988749895),
                new Edge(graph.toMappedNodeId("b"), graph.toMappedNodeId("c"), 2.8284271247461903),
                new Edge(graph.toMappedNodeId("c"), graph.toMappedNodeId("e"), 997.0045135304052)
            );

            assertThat(result.edges().toArray())
                .usingElementComparator(new UndirectedEdgeComparator())
                .containsExactlyInAnyOrderElementsOf(expected);

            assertThat(result.totalDistance())
                .isCloseTo(
                    expected.stream()
                        .mapToDouble(Edge::distance)
                        .sum(),
                    Offset.offset(1e-12)
                );
        }

        @Test
        void shouldLogProgress(){

            var progressTask = HDBScanProgressTrackerCreator.boruvkaTask("boruvka",graph.nodeCount());
            var log = new GdsTestLog();
            var progressTracker = new TaskProgressTracker(progressTask, new LoggerForProgressTrackingAdapter(log), new Concurrency(1), EmptyTaskRegistryFactory.INSTANCE);

            var nodePropertyValues = graph.nodeProperties("point");
            var kdTree = new KdTreeBuilder(graph,
                nodePropertyValues,
                1,
                1,
                ProgressTracker.NULL_TRACKER
            ).build();

            BoruvkaMST.createWithZeroCores(
                nodePropertyValues,
                kdTree,
                graph.nodeCount(),
                new Concurrency(1),
                progressTracker
            ).compute();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                "boruvka :: Start",
                "boruvka 20%",
                "boruvka 40%",
                "boruvka 60%",
                "boruvka 80%",
                "boruvka 100%",
                "boruvka :: Finished"
                );

        }

    }


}
