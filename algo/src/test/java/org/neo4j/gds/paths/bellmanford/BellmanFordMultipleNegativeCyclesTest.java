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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BellmanFordMultipleNegativeCyclesTest {
    @GdlExtension
    @Nested
    class TwoDisjointCyclesCase {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE " +
            "  (a0)," +
            "  (a1)," +
            "  (a2)," +
            "  (a3)," +
            "  (a4)," +
            "  (a5)," +
            "  (a0)-[:R {weight:  1.0}]->(a1)," +
            "  (a0)-[:R {weight:  1.0}]->(a2)," +

            "  (a1)-[:R {weight:  -1.0}]->(a3)," +
            "  (a3)-[:R {weight:  -2.0}]->(a4)," +
            "  (a4)-[:R {weight:  2.0}]->(a1)," + //Negative Cycle 1  a1-a3-a4

            "  (a2)-[:R {weight:  -100.0}]->(a5)," +
            "  (a5)-[:R {weight:  5.0}]->(a6)," +
            "  (a6)-[:R {weight:  6.0}]->(a2)"; //Negative Cycle 2  a2-a5-a6

        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;


        @Test
        void shouldReturnNegativeCycles() {
            long[] a = new long[]{
                idFunction.of("a0"),
                idFunction.of("a1"),
                idFunction.of("a2"),
                idFunction.of("a3"),
                idFunction.of("a4"),
                idFunction.of("a5"),
                idFunction.of("a6"),
            };
            var result = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, a[0], 4).compute();

            assertThat(result.containsNegativeCycle()).isTrue();
            var negativeCycles = result.negativeCycles();
            assertThat(negativeCycles).hasSize(2).
                containsExactlyInAnyOrder(List.of(a[3], a[4], a[1]), List.of(a[5], a[6], a[2]));

        }
    }

    @GdlExtension
    @Nested
    class LoopyTest {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE " +
            "  (a1)," +
            "  (a2)," +
            "  (a3)," +

            "  (a1)-[:R {weight:  -20.0}]->(a2)," +
            "  (a2)-[:R {weight:  10.0}]->(a3)," +

            "  (a3)-[:R {weight:  5.0}]->(a1)," +
            "  (a3)-[:R {weight:  -15.0}]->(a2)";


        @Inject
        private TestGraph graph;

        @Inject
        private IdFunction idFunction;


        @Test
        void shouldReturnNegativeCycles() {
            long[] a = new long[]{
                idFunction.of("a1"),
                idFunction.of("a2"),
                idFunction.of("a3"),
            };
            var result = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, a[0], 4).compute();

            assertThat(result.containsNegativeCycle()).isTrue();
            var negativeCycles = result.negativeCycles();
            assertThat(negativeCycles).
                containsExactlyInAnyOrder(List.of(2l, 1l));

        }
    }
}
