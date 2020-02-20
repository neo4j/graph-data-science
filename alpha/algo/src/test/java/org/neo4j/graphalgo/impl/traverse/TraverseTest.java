/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.traverse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.impl.traverse.Traverse.ExitPredicate.Result;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.impl.traverse.Traverse.DEFAULT_AGGREGATOR;

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
class TraverseTest extends AlgoTestBase {

    private static GraphsByRelationshipType graphs;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (g:Node {name:'g'})\n" +
                "CREATE" +
                " (a)-[:REL {cost:2.0}]->(b),\n" +
                " (a)-[:REL {cost:1.0}]->(c),\n" +
                " (b)-[:REL {cost:1.0}]->(d),\n" +
                " (c)-[:REL {cost:2.0}]->(d),\n" +
                " (d)-[:REL {cost:1.0}]->(e),\n" +
                " (d)-[:REL {cost:2.0}]->(f),\n" +
                " (e)-[:REL {cost:2.0}]->(g),\n" +
                " (f)-[:REL {cost:1.0}]->(g)";

        runQuery(cypher);

        graphs = new StoreLoaderBuilder()
            .api(db)
            .addNodeLabel("Node")
            .putRelationshipProjectionsWithIdentifier("REL_OUT", RelationshipProjection.of("REL", Orientation.NATURAL, Aggregation.NONE))
            .putRelationshipProjectionsWithIdentifier("REL_IN", RelationshipProjection.of("REL", Orientation.REVERSE, Aggregation.NONE))
            .putRelationshipProjectionsWithIdentifier("REL_BOTH", RelationshipProjection.of("REL", Orientation.UNDIRECTED, Aggregation.NONE))
            .addRelationshipProperty(PropertyMapping.of("cost", Double.MAX_VALUE))
            .build()
            .graphs(HugeGraphFactory.class);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    private long id(String name) {
        final Node[] node = new Node[1];
        runQuery("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n", row -> node[0] = row.getNode("n"));
        return node[0].getId();
    }

    private String name(long id) {
        final String[] node = new String[1];
        runQuery(
            "MATCH (n:Node) WHERE id(n) = " + id + " RETURN n.name as name",
            row -> node[0] = row.getString("name")
        );
        return node[0];
    }

    /**
     * bfs on outgoing rels. until target 'd' is reached
     */
    @Test
    void testBfsToTargetOut() {
        long source = id("a");
        long target = id("d");
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.bfs(
            graph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            (s, t, w) -> 1.
        ).compute().resultNodes();

        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testDfsToTargetOut() {
        long source = id("a");
        long target = id("g");
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.dfs(
            graph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR
        ).compute().resultNodes();

        assertEquals(5, nodes.length);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testExitConditionNeverTerminates() {
        long source = id("a");
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.dfs(
            graph,
            source,
            (s, t, w) -> Result.FOLLOW,
            DEFAULT_AGGREGATOR
        ).compute().resultNodes();
        assertEquals(7, nodes.length); // should contain all nodes
    }

    /**
     * dfs on incoming rels. from 'g' until 'a' is reached. exit function
     * immediately returns if target is reached
     */
    @Test
    void testDfsToTargetIn() {
        long source = id("g");
        long target = id("a");
        Graph graph = graphs.getGraphProjection("REL_IN", Optional.of("cost"));
        long[] nodes = Traverse.dfs(
            graph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR
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
        long source = id("g");
        long target = id("a");
        Graph graph = graphs.getGraphProjection("REL_IN", Optional.of("cost"));
        long[] nodes = Traverse.bfs(
            graph,
            source,
            (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
            DEFAULT_AGGREGATOR
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
        long source = id("a");
        double maxHops = 3.;
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.bfs(
            graph,
            source,
            (s, t, w) -> w >= maxHops ? Result.CONTINUE : Result.FOLLOW,
            (s, t, w) -> w + 1.
        ).compute().resultNodes();
        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    @Test
    void testBfsMaxCostOut() {
        long source = id("a");
        double maxCost = 3.;
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.bfs(
            graph,
            source,
            (s, t, w) -> w > maxCost ? Result.CONTINUE : Result.FOLLOW,
            (s, t, w) -> {
                final double v = graph.relationshipProperty(s, t, Double.NaN);
                return w + v;
            }
        ).compute().resultNodes();
        assertEquals(4, nodes.length);
    }

    @Test
    void testDfsMaxCostOut() {
        long source = id("a");
        double maxCost = 3.;
        Graph graph = graphs.getGraphProjection("REL_OUT", Optional.of("cost"));
        long[] nodes = Traverse.dfs(
            graph,
            source,
            (s, t, w) -> w > maxCost ? Result.CONTINUE : Result.FOLLOW,
            (s, t, w) -> {
                final double v = graph.relationshipProperty(s, t, Double.NaN);
                return w + v;
            }
        ).compute().resultNodes();
        assertEquals(4, nodes.length);
    }

    /**
     * test if all both arrays contain the same nodes. not necessarily in
     * same order
     */
    void assertContains(String[] expected, long[] given) {
        Arrays.sort(given);
        assertEquals(expected.length, given.length, "expected " + Arrays.toString(expected) + " | given [" + toNameString(given) + "]");

        for (String ex : expected) {
            final long id = id(ex);
            if (Arrays.binarySearch(given, id) == -1) {
                fail(ex + " not in " + Arrays.toString(expected));
            }
        }
    }

    String toNameString(long[] nodes) {
        final StringBuilder builder = new StringBuilder();
        for (long node : nodes) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(name(node));
        }
        return builder.toString();
    }
}
