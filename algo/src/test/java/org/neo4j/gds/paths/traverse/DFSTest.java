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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.traverse.ExitPredicate.Result;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
class DFSTest {


    @GdlGraph(graphNamePrefix = "natural")
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String CYPHER =
        "CREATE " +
        "  (a:Node)" +
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
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testDfsToTargetOut() {
        long source = naturalGraph.toMappedNodeId("a");
        long target = naturalGraph.toMappedNodeId("g");
        long[] nodes = new DFS(
            naturalGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            DfsBaseConfig.NO_MAX_DEPTH,
            ProgressTracker.NULL_TRACKER
        ).compute().toArray();

        assertThat(nodes).isEqualTo(
            Stream.of("a", "c", "d", "f", "g").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testExitConditionNeverTerminates() {
        long source = naturalGraph.toMappedNodeId("a");
        long[] nodes = new DFS(
            naturalGraph,
            source,
            (s, t, w) -> Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            DfsBaseConfig.NO_MAX_DEPTH,
            ProgressTracker.NULL_TRACKER
        ).compute().toArray();

        assertThat(nodes).isEqualTo(
            Stream.of("a", "c", "d", "f", "g", "e", "b").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    /**
     * dfs on incoming rels. from 'g' until 'a' is reached. exit function
     * immediately returns if target is reached
     */
    @Test
    void testDfsToTargetIn() {
        long source = reverseGraph.toMappedNodeId("g");
        long target = reverseGraph.toMappedNodeId("a");
        long[] nodes = new DFS(
            reverseGraph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            DfsBaseConfig.NO_MAX_DEPTH,
            ProgressTracker.NULL_TRACKER
        ).compute().toArray();

        assertThat(nodes).isEqualTo(
            Stream.of("g", "f", "d", "c", "a").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    @Test
    void testDfsWithDepth() {
        long source = naturalGraph.toMappedNodeId("a");
        long[] nodes = new DFS(
            naturalGraph,
            source,
            (s, t, w) -> Result.FOLLOW,
            new OneHopAggregator(),
            3,
            ProgressTracker.NULL_TRACKER
        ).compute().toArray();

        assertThat(nodes).isEqualTo(
            Stream.of("a", "c", "d", "f", "e", "b").mapToLong(naturalGraph::toMappedNodeId).toArray()
        );
    }

    @Test
    void testDfsOnLoopGraph() {
        new DFS(
            loopGraph,
            0,
            (s, t, w) -> Result.FOLLOW,
            Aggregator.NO_AGGREGATION,
            DfsBaseConfig.NO_MAX_DEPTH,
            ProgressTracker.NULL_TRACKER
        ).compute();
    }
}
