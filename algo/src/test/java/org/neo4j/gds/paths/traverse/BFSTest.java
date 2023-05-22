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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.neo4j.gds.paths.traverse.ExitPredicate.Result;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

/**
 * Graph:
 *
 * (b)   (e)
 * 2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 * 1\ 2/ 1\ 2/
 * (c)   (f)
 */

@GdlExtension
class BFSTest {

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
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testBfsToTargetOut(int concurrency) {
        long source = naturalGraph.toMappedNodeId("a");
        long target = naturalGraph.toMappedNodeId("d");
        long[] nodes = BFS.create(
            naturalGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            (s, t, w) -> 1.,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute().toArray();

        // algorithms return mapped ids
        assertThat(nodes).isEqualTo(
            Stream.of("a", "b", "c", "d").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    /**
     * bfs on incoming rels. from 'g' until taregt 'a' is reached.
     * result set should contain all nodes since both nodes lie
     * on the ends of the graph
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testBfsToTargetIn(int concurrency) {
        long source = reverseGraph.toMappedNodeId("g");
        long target = reverseGraph.toMappedNodeId("a");
        long[] nodes = BFS.create(
            reverseGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute().toArray();
        assertEquals(7, nodes.length);
    }

    /**
     * BFS until maxDepth is reached. The exit function does
     * not immediately exit if maxHops is reached, but
     * continues to check the other nodes that might have
     * lower depth
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testBfsMaxDepthOut(int concurrency) {
        long source = naturalGraph.toMappedNodeId("a");
        long maxHops = 4;
        long[] nodes = BFS.create(
            naturalGraph,
            source,
            (s, t, w) -> w >= maxHops ? Result.CONTINUE : Result.FOLLOW,
            (s, t, w) -> w + 1.,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            maxHops - 1
        ).compute().toArray();

        assertThat(nodes).isEqualTo(
            Stream.of("a", "b", "c", "d", "e", "f").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testBfsOnLoopGraph(int concurrency) {
        BFS.create(loopGraph, 0,
            (s, t, w) -> Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldLogProgress(int concurrency) {
        var progressTask = Tasks.leaf("BFS", naturalGraph.relationshipCount());
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);
        BFS.create(
            naturalGraph,
            0,
            (s, t, w) -> Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            concurrency,
            progressTracker,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute();
        var messagesInOrder = testLog.getMessages(INFO);

        assertThat(messagesInOrder)
            .extracting(removingThreadId())
            .containsSequence(
                "BFS :: Start",
                "BFS 12%",
                "BFS 37%",
                "BFS 50%",
                "BFS 75%",
                "BFS 87%",
                "BFS 100%",
                "BFS :: Finished"
            );
    }
}
