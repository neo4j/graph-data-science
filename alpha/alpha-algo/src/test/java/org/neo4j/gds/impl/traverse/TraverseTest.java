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
package org.neo4j.gds.impl.traverse;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.impl.traverse.Traverse.ExitPredicate.Result;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.impl.traverse.Traverse.DEFAULT_AGGREGATOR;

/**
 *
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 */
@GdlExtension
class TraverseTest {


    @GdlGraph(graphNamePrefix = "natural")
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String CYPHER =
        "CREATE (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +

        ", (a)-[:REL {cost:2.0}]->(b)" +
        ", (a)-[:REL {cost:1.0}]->(c)" +
        ", (b)-[:REL {cost:1.0}]->(d)" +
        ", (c)-[:REL {cost:2.0}]->(d)" +
        ", (d)-[:REL {cost:1.0}]->(e)" +
        ", (d)-[:REL {cost:2.0}]->(f)" +
        ", (e)-[:REL {cost:2.0}]->(g)" +
        ", (f)-[:REL {cost:1.0}]->(g)";

    @GdlGraph(graphNamePrefix = "loop")
    private static final String LOOP_CYPHER =
        "CREATE (a)-[:REL]->(b)-[:REL]->(a)";


    @Inject
    private static TestGraph naturalGraph;

    @Inject
    private static TestGraph reverseGraph;

    @Inject
    private static TestGraph undirectedGraph;

    @Inject
    private static TestGraph loopGraph;

    /**
     * bfs on outgoing rels. until target 'd' is reached
     */
    @Test
    void testBfsToTargetOut() {
        long source = naturalGraph.toMappedNodeId("a");
        long target = naturalGraph.toMappedNodeId("d");
        long[] nodes = Traverse.bfs(
            naturalGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            (s, t, w) -> 1.,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();

        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testDfsToTargetOut() {
        long source = naturalGraph.toMappedNodeId("a");
        long target = naturalGraph.toMappedNodeId("g");
        long[] nodes = Traverse.dfs(
            naturalGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();

        assertEquals(5, nodes.length);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testExitConditionNeverTerminates() {
        long source = naturalGraph.toMappedNodeId("a");
        long[] nodes = Traverse.dfs(
            naturalGraph,
            source,
            (s, t, w) -> Result.FOLLOW,
            DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();
        assertEquals(7, nodes.length); // should contain all nodes
    }

    /**
     * dfs on incoming rels. from 'g' until 'a' is reached. exit function
     * immediately returns if target is reached
     */
    @Test
    void testDfsToTargetIn() {
        long source = reverseGraph.toMappedNodeId("g");
        long target = reverseGraph.toMappedNodeId("a");
        long[] nodes = Traverse.dfs(
            reverseGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();
        assertEquals(5, nodes.length);
    }

    /**
     * bfs on incoming rels. from 'g' until taregt 'a' is reached.
     * result set should contain all nodes since both nodes lie
     * on the ends of the graph
     */
    @Test
    void testBfsToTargetIn() {
        long source = reverseGraph.toMappedNodeId("g");
        long target = reverseGraph.toMappedNodeId("a");
        long[] nodes = Traverse.bfs(
            reverseGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();
        assertEquals(7, nodes.length);
    }

    /**
     * BFS until maxDepth is reached. The exit function does
     * not immediately exit if maxHops is reached, but
     * continues to check the other nodes that might have
     * lower depth
     */
    @Test
    void testBfsMaxDepthOut() {
        long source = naturalGraph.toMappedNodeId("a");
        double maxHops = 3.;
        long[] nodes = Traverse.bfs(
            naturalGraph,
            source,
            (s, t, w) -> w >= maxHops ? Result.CONTINUE : Result.FOLLOW,
            (s, t, w) -> w + 1.,
            ProgressTracker.NULL_TRACKER
        ).compute().resultNodes();
        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    @Test
    void testBfsOnLoopGraph() {
        Traverse.bfs(loopGraph, 0,
            (s, t, w) -> Result.FOLLOW,
            Traverse.DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute();
    }

    @Test
    void testDfsOnLoopGraph() {
        Traverse.dfs(
            loopGraph,
            0,
            (s, t, w) -> Result.FOLLOW,
            Traverse.DEFAULT_AGGREGATOR,
            ProgressTracker.NULL_TRACKER
        ).compute();
    }

    /**
     * test if all both arrays contain the same nodes. not necessarily in
     * same order
     */
    void assertContains(String[] expected, long[] given) {
        Arrays.sort(given);
        assertEquals(
            expected.length,
            given.length,
            "expected " + Arrays.toString(expected) + " | given " + Arrays.toString(given)
        );

        for (String ex : expected) {
            final long id = naturalGraph.toMappedNodeId(ex);
            if (Arrays.binarySearch(given, id) == -1) {
                fail(ex + " not in " + Arrays.toString(expected));
            }
        }
    }

    @Test
    void shouldLogProgress() {
        //TODO:  Replace with TraverseFactory call
        var progressTask = Tasks.leaf("BFS", naturalGraph.relationshipCount());
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);
        Traverse.bfs(
            naturalGraph,
            0,
            (s, t, w) -> Result.FOLLOW,
            Traverse.DEFAULT_AGGREGATOR,
            progressTracker
        ).compute();
        List<AtomicLong> progresses = progressTracker.getProgresses();
        assertEquals(1, progresses.size());
        assertEquals(naturalGraph.relationshipCount(), progresses.get(0).get());
        var messagesInOrder = testLog.getMessages(INFO);

        assertThat(messagesInOrder)
            .extracting(removingThreadId())
            .containsSequence(
                "BFS :: Start",
                "BFS 25%",  //a-> b,a->c | 2 = 2/8 = 0.25
                "BFS 37%",  //b-> d      |+1 = 3/8 = 0.375
                "BFS 50%",  //c-> d      |+1 = 4/8 = 0.5
                "BFS 75%",  //d-> e,d->f |+2 = 6/8 = 0.75
                "BFS 87%",  //e->g       |+1 = 7/8 = 0.87
                "BFS 100%", //f->g       |+1 = 8/8 = 1.00
                "BFS :: Finished"
            );


    }
}
