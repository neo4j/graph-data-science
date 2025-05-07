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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BellmanFordNegativeCyclesTest {
    @GdlExtension
    @Nested
    class LoopsWithOrderingIssue{
        @GdlGraph(idOffset =  0)
        private static final String DB_CYPHER =
            """  
                      CREATE
                      (a0:Node),
                      (a1:Node),
                      (a2:Node),
                      (a3:Node),
                      (a4:Node),
                      (a1)-[:R{w:0.02}]->(a3),
                      (a2)-[:R{w:-0.32}]->(a1),
                      (a2)-[:R{w:0.24}]->(a3),
                      (a3)-[:R{w:-0.60}]->(a2)
               """;
        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFindUniqueCycle() {

            var bellmanFord = new BellmanFord(
                graph,
                ProgressTracker.NULL_TRACKER,
                3,
                true,
                true,
                new Concurrency(1),
                DefaultPool.INSTANCE
            ).compute();

            assertThat(bellmanFord.containsNegativeCycle()).isTrue();
            var negCycles = bellmanFord.negativeCycles();
            var paths = negCycles.pathSet();
            assertThat(paths).hasSize(1);
            var singlePath = paths.stream().findFirst().get();
            assertThat(singlePath.totalCost()).isCloseTo(-0.9, Offset.offset(1e-5));

        }
    }

    @GdlExtension
    @Nested
    class LoopsWithOrderingIssue2{
        @GdlGraph(idOffset =  0)
        private static final String DB_CYPHER =
            """  
                      CREATE
                      (a0)
                      (a1)
                      (a2)
                      (a3)
                      (a4)
                      (a1)-[:R{w:0.78}]->(a2),
                      (a1)-[:R{w:-0.88}]->(a3),
                      (a1)-[:R{w:0.90}]->(a4),
                      (a2)-[:R{w:-0.84}]->(a1),
                      (a2)-[:R{w:0.94}]->(a3),
                      (a2)-[:R{w:-0.35}]->(a4),
                      (a3)-[:R{w:0.81}]->(a1),
                      (a3)-[:R{w:-0.11}]->(a4),
                      (a4)-[:R{w:0.53}]->(a2)
               """;
        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFindCorrectLoop() {

            var bellmanFord = new BellmanFord(
                graph,
                ProgressTracker.NULL_TRACKER,
                2,
                true,
                true,
                new Concurrency(1),
                DefaultPool.INSTANCE
            ).compute();

            assertThat(bellmanFord.containsNegativeCycle()).isTrue();
            var negCycles = bellmanFord.negativeCycles();
            var paths = negCycles.pathSet();
            assertThat(paths).isNotEmpty();
            var firstPath = paths.stream().findFirst().orElseThrow();
            assertThat(firstPath.nodeIds()).contains(4,2,1,3);
            assertThat(firstPath.totalCost()).isCloseTo(-1.3,Offset.offset(1e-5));


        }
    }

    @GdlExtension
    @Nested
    class LoopsWithOrderingIssue3{
        @GdlGraph(idOffset =  0)
        private static final String DB_CYPHER =
            """  
                     (a0)
                     (a1)
                     (a2)
                     (a3)
                     (a4)
                     (a5)
                     (a1)-[:R{w:0.34}]->(a4),
                     (a1)-[:R{w:0.77}]->(a5),
                     (a2)-[:R{w:0.90}]->(a1),
                     (a2)-[:R{w:0.99}]->(a3),
                     (a2)-[:R{w:-0.23}]->(a4),
                     (a2)-[:R{w:0.18}]->(a5),
                     (a3)-[:R{w:-0.40}]->(a2),
                     (a3)-[:R{w:0.35}]->(a4),
                     (a4)-[:R{w:0.93}]->(a2),
                     (a4)-[:R{w:-0.26}]->(a3),
                     (a4)-[:R{w:-0.17}]->(a5),
                     (a5)-[:R{w:-0.31}]->(a1)
               """;
        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFindCorrectLoop() {

            var bellmanFord = new BellmanFord(
                graph,
                ProgressTracker.NULL_TRACKER,
                4,
                true,
                true,
                new Concurrency(1),
                DefaultPool.INSTANCE
            ).compute();

            assertThat(bellmanFord.containsNegativeCycle()).isTrue();
            var negCycles = bellmanFord.negativeCycles();
            var paths = negCycles.pathSet();
            assertThat(paths).isNotEmpty();
            var firstPath = paths.stream().findFirst().orElseThrow();
            assertThat(firstPath.nodeIds()).contains(4,2,3);
            assertThat(firstPath.totalCost()).isCloseTo(-0.89,Offset.offset(1e-5));

        }
    }

    @GdlExtension
    @Nested
    class LoopsWithOrderingIssue4{
        @GdlGraph(idOffset =  0)
        private static final String DB_CYPHER =
            """  
                     (a0)
                     (a1)
                     (a2)
                     (a3)
                     (a4)
                     (a1)-[:R{w:-0.2}]->(a3),
                     (a1)-[:R{w:0.7}]->(a4),
                     (a2)-[:R{w:0.01}]->(a1),
                     (a2)-[:R{w:-0.5}]->(a4),
                     (a3)-[:R{w:0.4}]->(a2),
                     (a3)-[:R{w:0.8}]->(a4),
                     (a4)-[:R{w:0.09}]->(a2)
               """;
        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFindCorrectLoop() {

            var bellmanFord = new BellmanFord(
                graph,
                ProgressTracker.NULL_TRACKER,
                1,
                true,
                true,
                new Concurrency(1),
                DefaultPool.INSTANCE
            ).compute();

            assertThat(bellmanFord.containsNegativeCycle()).isTrue();
            var negCycles = bellmanFord.negativeCycles();
            var paths = negCycles.pathSet();
            assertThat(paths).isNotEmpty();
            var firstPath = paths.stream().findFirst().orElseThrow();
            assertThat(firstPath.nodeIds()).contains(4,2);
            assertThat(firstPath.totalCost()).isCloseTo(-0.41,Offset.offset(1e-5));
        }
    }

    @GdlExtension
    @Nested
    class LoopsWithOrderingIssue5{
        @GdlGraph(idOffset =  0)
        private static final String DB_CYPHER =
            """  
                     (a0)
                     (a1)
                     (a2)
                     (a0)-[:R{w:1.0}]->(a1),
                     (a0)-[:R{w:1.0}]->(a2),
                     (a1)-[:R{w:-1000.0}]->(a2),
                     (a2)-[:R{w:-1.0}]->(a3),
                     (a3)-[:R{w:-1.0}]->(a2)
               """;
        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFindCycleAfterReprocessingIt() {

            var bellmanFord = new BellmanFord(
                graph,
                ProgressTracker.NULL_TRACKER,
                1,
                true,
                true,
                new Concurrency(1),
                DefaultPool.INSTANCE
            ).compute();

            assertThat(bellmanFord.containsNegativeCycle()).isTrue();
            var negCycles = bellmanFord.negativeCycles();
            var paths = negCycles.pathSet();
            assertThat(paths).isNotEmpty();
              var singlePath = paths.stream().findFirst().get();
             assertThat(singlePath.totalCost()).isCloseTo(-2, Offset.offset(1e-5));
            assertThat(singlePath.nodeIds()).contains(2,3);
        }
    }

}
