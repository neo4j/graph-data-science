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
package org.neo4j.gds.impl;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressLogger;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;


/**         5     5      5
 *      (A)---(B)---(C)----.
 *    5/ 2\2  2 \2  2 \2  2 \
 *  (S)---(G)---(H)---(I)---(X)--//->(S)
 *    3\  /3 3  /3 3  /3 3  /
 *      (D)---(E)---(F)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
@GdlExtension
final class ShortestPathDeltaSteppingTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (s:Node)" +
        ", (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (x:Node)" +
        ", (z:Node)" +

        ", (s)-[:TYPE {cost:5}]->(a)" +
        ", (a)-[:TYPE {cost:5}]->(b)" +
        ", (b)-[:TYPE {cost:5}]->(c)" +
        ", (c)-[:TYPE {cost:5}]->(x)" +

        ", (a)-[:TYPE {cost:2}]->(g)" +
        ", (b)-[:TYPE {cost:2}]->(h)" +
        ", (c)-[:TYPE {cost:2}]->(i)" +

        ", (s)-[:TYPE {cost:3}]->(d)" +
        ", (d)-[:TYPE {cost:3}]->(e)" +
        ", (e)-[:TYPE {cost:3}]->(f)" +
        ", (f)-[:TYPE {cost:3}]->(x)" +

        ", (d)-[:TYPE {cost:3}]->(g)" +
        ", (e)-[:TYPE {cost:3}]->(h)" +
        ", (f)-[:TYPE {cost:3}]->(i)" +

        ", (s)-[:TYPE {cost:2}]->(g)" +
        ", (g)-[:TYPE {cost:2}]->(h)" +
        ", (h)-[:TYPE {cost:2}]->(i)" +
        ", (i)-[:TYPE {cost:2}]->(x)" +

        ", (x)-[:TYPE {cost:2}]->(s)"; // create cycle

    @GdlGraph(graphNamePrefix = "largeWeights")
    private static final String LARGE_WEIGHTS_CYPHER = "CREATE (a)-[:TYPE {cost: 100000}]->(b)";

    @Inject
    private static TestGraph graph;

    @Inject
    private static TestGraph largeWeightsGraph;

    @Test
    void testSequential() {
        var sssp = new ShortestPathDeltaStepping(graph, graph.toOriginalNodeId("s"), 3, ProgressTracker.NULL_TRACKER);

        var sp = sssp.compute().getShortestPaths();

        assertEquals(8, sp[Math.toIntExact(graph.toMappedNodeId("x"))],0.1);
    }

    @Test
    void testParallel() {
        var sssp = new ShortestPathDeltaStepping(graph, graph.toOriginalNodeId("s"), 3, ProgressTracker.NULL_TRACKER)
                .withExecutorService(Executors.newFixedThreadPool(3));

        var sp = sssp.compute().getShortestPaths();

        assertEquals(8, sp[Math.toIntExact(graph.toMappedNodeId("x"))],0.1);
    }

    @Test
    void distanceToNodeInDifferentComponentShouldBeInfinity() {
        var sssp = new ShortestPathDeltaStepping(graph, graph.toOriginalNodeId("s"),3, ProgressTracker.NULL_TRACKER);

        var sp = sssp.compute().getShortestPaths();

        assertEquals(Double.POSITIVE_INFINITY, sp[Math.toIntExact(graph.toMappedNodeId("z"))],0.1);
    }

    @Test
    void handleLargeDistances() {
        var sssp = new ShortestPathDeltaStepping(
            largeWeightsGraph,
            largeWeightsGraph.toOriginalNodeId("a"),
            3,
            ProgressTracker.NULL_TRACKER
        );

        var sp = sssp.compute().getShortestPaths();

        assertNotEquals(Double.POSITIVE_INFINITY, sp[Math.toIntExact(largeWeightsGraph.toMappedNodeId("b"))]);
    }

    @Test
    void failOnLowDeltaAndLargeDistance() {
        var sssp = new ShortestPathDeltaStepping(
            largeWeightsGraph,
            largeWeightsGraph.toOriginalNodeId("a"),
            1e-5,
            ProgressTracker.NULL_TRACKER
        );

        assertThrows(ArithmeticException.class, () -> sssp.compute());
    }

    @Test
    void failOnTooSmallDelta() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ShortestPathDeltaStepping(
                largeWeightsGraph,
                largeWeightsGraph.toOriginalNodeId("a"),
                1e-12,
                ProgressTracker.NULL_TRACKER
            ));
    }

    @Test
    void testLogging() {
        var task = Tasks.leaf("My task");
        var progressLogger = new TestProgressLogger(task, 1);
        var progressTracker = new TaskProgressTracker(task, progressLogger, EmptyTaskRegistryFactory.INSTANCE);

        var algo = new ShortestPathDeltaStepping(
            graph,
            largeWeightsGraph.toOriginalNodeId("a"),
            .1,
            progressTracker
        );

        algo.compute();

        assertThat(progressLogger.getMessages(INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            // TODO add entries when open task log more (open PR by soeren
            .containsExactly(
                "My task :: Start",
                "My task 100%",
                "My task :: Finished"
            );
    }

}
