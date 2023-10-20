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
package org.neo4j.gds.dag.longestPath;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class WeightedDagLongestPathTest {
    private static DagLongestPathBaseConfig CONFIG = new DagLongestPathStreamConfigImpl.Builder()
        .concurrency(4)
        .build();

    @GdlGraph(graphNamePrefix = "basic")
    private static final String basicQuery =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-[:T {prop: 8.0}]->(n1)" +
        ", (n0)-[:T {prop: 5.0}]->(n2)" +
        ", (n2)-[:T {prop: 2.0}]->(n1)" +
        ", (n3)-[:T {prop: 8.0}]->(n0)";

    @Inject
    private TestGraph basicGraph;

    @Test
    void basicWeightedLongestPath() {
        IdFunction idFunction = basicGraph::toMappedNodeId;

        long[] a = new long[]{
            idFunction.of("n0"),
            idFunction.of("n1"),
            idFunction.of("n2"),
            idFunction.of("n3"),
        };

        var longestPath = new DagLongestPathFactory().build(
            basicGraph,
            CONFIG,
            ProgressTracker.NULL_TRACKER
        );

        PathFindingResult result = longestPath.compute();

        long[][] EXPECTED_PATHS = new long[4][];
        EXPECTED_PATHS[(int) a[0]] = new long[]{a[3], a[0]};
        EXPECTED_PATHS[(int) a[1]] = new long[]{a[3], a[0], a[1]};
        EXPECTED_PATHS[(int) a[2]] = new long[]{a[3], a[0], a[2]};
        EXPECTED_PATHS[(int) a[3]] = new long[]{a[3]};
        double[] EXPECTED_COSTS = new double[4];
        EXPECTED_COSTS[(int) a[0]] = 8;
        EXPECTED_COSTS[(int) a[1]] = 16;
        EXPECTED_COSTS[(int) a[2]] = 13;
        EXPECTED_COSTS[(int) a[3]] = 0;

        long counter = 0;
        for (var path : result.pathSet()) {
            counter++;
            int currentTargetNode = (int) path.targetNode();
            assertThat(path.nodeIds()).isEqualTo(EXPECTED_PATHS[currentTargetNode]);
            assertThat(EXPECTED_COSTS[currentTargetNode]).isEqualTo(path.totalCost());
        }
        assertThat(counter).isEqualTo(4L);
    }

    @Test
    void shouldLogProgress() {
        var lpFactory = new DagLongestPathFactory<>();
        var progressTask = lpFactory.progressTask(basicGraph, CONFIG);
        var log = Neo4jProxy.testLog();
        var testTracker = new TestProgressTracker(
            progressTask,
            log,
            CONFIG.concurrency(),
            EmptyTaskRegistryFactory.INSTANCE
        );

        var lp = lpFactory.build(basicGraph, CONFIG, testTracker);
        lp.compute().pathSet();

        String taskName = lpFactory.taskName();

        assertTrue(log.containsMessage(TestLog.INFO, taskName + " :: Start"));
        assertTrue(log.containsMessage(TestLog.INFO, taskName + " :: Finished"));
    }
}
