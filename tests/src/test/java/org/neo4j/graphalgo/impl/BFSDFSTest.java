/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.impl.Traverse.ExitPredicate.Result;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 *
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 *
 * @author mknblch
 */
class BFSDFSTest {

    private GraphDatabaseAPI db;

    private static Graph graph;

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
                " (a)-[:TYPE {cost:2.0}]->(b),\n" +
                " (a)-[:TYPE {cost:1.0}]->(c),\n" +
                " (b)-[:TYPE {cost:1.0}]->(d),\n" +
                " (c)-[:TYPE {cost:2.0}]->(d),\n" +
                " (d)-[:TYPE {cost:1.0}]->(e),\n" +
                " (d)-[:TYPE {cost:2.0}]->(f),\n" +
                " (e)-[:TYPE {cost:2.0}]->(g),\n" +
                " (f)-[:TYPE {cost:1.0}]->(g)";

        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withRelationshipProperties(PropertyMapping.of("cost", Double.MAX_VALUE))
                .withDirection(Direction.BOTH)
                .load(HugeGraphFactory.class);

    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    private long id(String name) {
        final Node[] node = new Node[1];
        db.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return node[0].getId();
    }

    private String name(long id) {
        final String[] node = new String[1];
        db.execute("MATCH (n:Node) WHERE id(n) = " + id + " RETURN n.name as name").accept(row -> {
            node[0] = row.getString("name");
            return false;
        });
        return node[0];
    }


    /**
     * bfs on outgoing rels. until target 'd' is reached
     */
    @Test
    void testBfsToTargetOut() {
        final long source = id("a");
        final long target = id("d");
        final long[] nodes = new Traverse(graph)
                .computeBfs(
                        source,
                        Direction.OUTGOING,
                        (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW,
                        (s, t, w) -> 1.);
        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testDfsToTargetOut() {
        final long source = id("a");
        final long target = id("g");
        final long[] nodes = new Traverse(graph)
                .computeDfs(
                        source,
                        Direction.OUTGOING,
                        (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW);
        System.out.println(Arrays.toString(nodes));
        assertEquals(5, nodes.length);
    }

    /**
     * dfs on outgoing rels. until taregt 'a' is reached. the exit function
     * immediately exits if target is reached
     */
    @Test
    void testExitConditionNeverTerminates() {
        final long source = id("a");
        final long[] nodes = new Traverse(graph)
                .computeDfs(
                        source,
                        Direction.OUTGOING,
                        (s, t, w) -> Result.FOLLOW);
        assertEquals(7, nodes.length); // should contain all nodes
    }

    /**
     * dfs on incoming rels. from 'g' until 'a' is reached. exit function
     * immediately returns if target is reached
     *
     */
    @Test
    void testDfsToTargetIn() {
        final long source = id("g");
        final long target = id("a");
        final long[] nodes = new Traverse(graph)
                .computeDfs(
                        source,
                        Direction.INCOMING,
                        (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW);
        System.out.println(Arrays.toString(nodes));
        assertEquals(5, nodes.length);
    }

    /**
     * bfs on incoming rels. from 'g' until taregt 'a' is reached.
     * result set should contain all nodes since both nodes lie
     * on the ends of the graph
     */
    @Test
    void testBfsToTargetIn() {
        final long source = id("g");
        final long target = id("a");
        final long[] nodes = new Traverse(graph)
                .computeBfs(
                        source,
                        Direction.INCOMING,
                        (s, t, w) -> t == target ? Result.BREAK : Result.FOLLOW);
        System.out.println(Arrays.toString(nodes));
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
        final long source = id("a");
        final double maxHops = 3.;
        final long[] nodes = new Traverse(graph)
                .computeBfs(
                        source,
                        Direction.OUTGOING,
                        (s, t, w) -> w >= maxHops ? Result.CONTINUE : Result.FOLLOW,
                        (s, t, w) -> {
                            System.out.println(s + " -> " + t + " : " + (w + 1));
                            return w + 1.;
                        });
        System.out.println(Arrays.toString(nodes));
        assertContains(new String[]{"a", "b", "c", "d"}, nodes);
    }

    @Test
    void testBfsMaxCostOut() {
        final long source = id("a");
        final double maxCost = 3.;
        final long[] nodes = new Traverse(graph)
                .computeBfs(
                        source,
                        Direction.OUTGOING,
                        (s, t, w) -> w > maxCost ? Result.CONTINUE : Result.FOLLOW,
                        (s, t, w) -> {
                            final double v = graph.relationshipValue(s, t, Double.NaN);
                            System.out.println(s + " -> " + t + " : " + (w + v));
                            return w + v;
                        });
        System.out.println(Arrays.toString(nodes));
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
