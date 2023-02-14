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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BellmanFordTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0)," +
        "  (a1)," +
        "  (a2)," +
        "  (a3)," +
        "  (a4)," +
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: -1.0}]->(a2)," +
        "  (a0)-[:R {weight: 10.0}]->(a3), " +
        "  (a3)-[:R {weight: -8.0}]->(a4), "  +
        "  (a1)-[:R {weight: 3.0}]->(a4) " ;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldComputeShortestPathsWithoutLoops(){
        long[] a = new long[]{idFunction.of("a0"), idFunction.of("a1"), idFunction.of("a2"), idFunction.of("a3"), idFunction.of(
            "a4")};
        var result = new BellmanFord(graph, ProgressTracker.NULL_TRACKER, a[0], 1).compute();
        long[][] EXPECTED_PATHS = new long[5][];
        EXPECTED_PATHS[(int) a[0]] = new long[]{a[0]};
        EXPECTED_PATHS[(int) a[1]] = new long[]{a[0], a[1]};
        EXPECTED_PATHS[(int) a[2]] = new long[]{a[0], a[2]};
        EXPECTED_PATHS[(int) a[3]] = new long[]{a[0], a[3]};
        EXPECTED_PATHS[(int) a[4]] = new long[]{a[0], a[3], a[4]};
        double[] EXPECTED_COSTS = new double[5];
        EXPECTED_COSTS[(int) a[0]] = 0;
        EXPECTED_COSTS[(int) a[1]] = 1;
        EXPECTED_COSTS[(int) a[2]] = -1;
        EXPECTED_COSTS[(int) a[3]] = 10;
        EXPECTED_COSTS[(int) a[4]] = 2;

        long counter = 0;
        for (var path : result.pathSet()) {
            counter++;
            int currentTargetNode = (int) path.targetNode();
            assertThat(path.nodeIds()).isEqualTo(EXPECTED_PATHS[currentTargetNode]);
            assertThat(EXPECTED_COSTS[currentTargetNode]).isEqualTo(path.totalCost());
        }
        assertThat(counter).isEqualTo(5L);
    }

}
