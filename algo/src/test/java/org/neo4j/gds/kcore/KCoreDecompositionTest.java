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
package org.neo4j.gds.kcore;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class KCoreDecompositionTest {

    @GdlExtension
    @Nested
    class BlossomGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
            "  (z:node)," +
            "  (a:node)," +
            "  (b:node)," +
            "  (c:node)," +
            "  (d:node)," +
            "  (e:node)," +
            "  (f:node)," +
            "  (g:node)," +
            "  (h:node)," +

            "(a)-[:R]->(b)," +
            "(b)-[:R]->(c)," +
            "(c)-[:R]->(d)," +
            "(d)-[:R]->(e)," +
            "(e)-[:R]->(f)," +
            "(f)-[:R]->(g)," +
            "(g)-[:R]->(h)," +
            "(h)-[:R]->(c)";


        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @ValueSource(ints = {1, 4})
        void shouldComputeCoreDecomposition(int concurrency) {

            var kcore = new KCoreDecomposition(graph, concurrency, ProgressTracker.NULL_TRACKER, 1).compute();
            assertThat(kcore.degeneracy()).isEqualTo(2);
            var coreValues = kcore.coreValues();

            assertThat(coreValues.get(graph.toMappedNodeId("z"))).isEqualTo(0);
            assertThat(coreValues.get(graph.toMappedNodeId("a"))).isEqualTo(1);
            assertThat(coreValues.get(graph.toMappedNodeId("b"))).isEqualTo(1);
            assertThat(coreValues.get(graph.toMappedNodeId("c"))).isEqualTo(2);
            assertThat(coreValues.get(graph.toMappedNodeId("d"))).isEqualTo(2);
            assertThat(coreValues.get(graph.toMappedNodeId("e"))).isEqualTo(2);
            assertThat(coreValues.get(graph.toMappedNodeId("f"))).isEqualTo(2);
            assertThat(coreValues.get(graph.toMappedNodeId("g"))).isEqualTo(2);
            assertThat(coreValues.get(graph.toMappedNodeId("h"))).isEqualTo(2);

        }

        @Test
        void shouldLogProgress() {
            var config = KCoreDecompositionStreamConfigImpl.builder().build();

            var factory = new KCoreDecompositionAlgorithmFactory<>();

            var progressTask = factory.progressTask(graph, config);
            var log = Neo4jProxy.testLog();
            var progressTracker = new TaskProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);

            factory
                .build(graph, config, progressTracker)
                .compute();

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "KCoreDecomposition :: Start",
                    "KCoreDecomposition 11%",
                    "KCoreDecomposition 33%",
                    "KCoreDecomposition 100%",
                    "KCoreDecomposition :: Finished"
                );
        }
    }

    // Graph is copied from https://www.researchgate.net/figure/Illustration-of-the-k-core-decomposition-of-a-small-network-The-sets-of-nodes-belonging_fig1_347212713
    //ids wrt to different color class are given based on clockwise order (starting from highest element (or highst and most right for red))
    @GdlExtension
    @Nested
    class ThreeCoreGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
            "  (z:node)," +
            "  (green1:node)," +
            "  (green2:node)," +
            "  (green3:node)," +
            "  (green4:node)," +
            "  (green5:node)," +
            "  (green6:node)," +
            "  (green7:node)," +
            "  (green8:node)," +
            "  (yellow1:node)," +
            "  (yellow2:node)," +
            "  (red1:node)," +
            "  (red2:node)," +
            "  (red3:node)," +
            "  (red4:node)," +
            "(green1)-[:R]->(green2)," +
            "(green2)-[:R]->(green3)," +
            "(green2)-[:R]->(yellow1)," +
            "(green4)-[:R]->(red1)," +
            "(green5)-[:R]->(yellow2)," +
            "(green6)-[:R]->(yellow2)," +
            "(green7)-[:R]->(yellow2)," +
            "(green8)-[:R]->(red3)," +
            "(yellow1)-[:R]->(red1)," +
            "(yellow1)-[:R]->(red4)," +
            "(yellow2)-[:R]->(red2)," +
            "(yellow2)-[:R]->(red3)," +
            "(red1)-[:R]->(red4)," +
            "(red1)-[:R]->(red2)," +
            "(red1)-[:R]->(red3)," +
            "(red2)-[:R]->(red4)," +
            "(red2)-[:R]->(red3)," +
            "(red3)-[:R]->(red4)";


        @Inject
        private TestGraph graph;

        @ParameterizedTest
        @ValueSource(ints = {1, 4})
        void shouldComputeCoreDecomposition(int concurrency) {
            IdFunction idFunction = graph::toMappedNodeId;

            var kcore = new KCoreDecomposition(graph, concurrency, ProgressTracker.NULL_TRACKER, 1).compute();
            assertThat(kcore.degeneracy()).isEqualTo(3);
            var coreValues = kcore.coreValues();

            assertThat(coreValues.get(idFunction.of("z"))).isEqualTo(0L);

            for (int i = 1; i <= 8; ++i) {
                assertThat(coreValues.get(idFunction.of("green" + i))).isEqualTo(1L);
            }

            assertThat(coreValues.get(idFunction.of("yellow1"))).isEqualTo(2L);
            assertThat(coreValues.get(idFunction.of("yellow2"))).isEqualTo(2L);

            for (int i = 1; i <= 4; ++i) {
                assertThat(coreValues.get(idFunction.of("red" + i))).isEqualTo(3L);
            }

        }
    }

    @GdlExtension
    @Nested
    class EmptyGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
            "  (a:node)," +
            "  (b:node)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldComputeCoreDecomposition() {

            var kcore = new KCoreDecomposition(graph, 1, ProgressTracker.NULL_TRACKER, 1).compute();
            assertThat(kcore.degeneracy()).isEqualTo(0);
            var coreValues = kcore.coreValues();

            assertThat(coreValues.get(graph.toMappedNodeId("a"))).isEqualTo(0L);
            assertThat(coreValues.get(graph.toMappedNodeId("b"))).isEqualTo(0L);

        }

    }
}
