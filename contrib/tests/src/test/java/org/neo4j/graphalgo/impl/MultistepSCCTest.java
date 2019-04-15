/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class MultistepSCCTest {


    private static GraphDatabaseAPI api;

    private static Graph graph;

    @BeforeClass
    public static void setup() {
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE (x:Node {name:'x'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(a),\n" +

                        " (d)-[:TYPE {cost:2}]->(e),\n" +
                        " (e)-[:TYPE {cost:2}]->(f),\n" +
                        " (f)-[:TYPE {cost:2}]->(d),\n" +

                        " (a)-[:TYPE {cost:2}]->(d),\n" +

                        " (g)-[:TYPE {cost:3}]->(h),\n" +
                        " (h)-[:TYPE {cost:3}]->(i),\n" +
                        " (i)-[:TYPE {cost:3}]->(g)";

        api = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(api)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (api != null) api.shutdown();
        graph = null;
    }

    public static int getMappedNodeId(String name) {
        final Node[] node = new Node[1];
        api.execute("MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n").accept(row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    private IntSet allNodes() {
        final IntScatterSet nodes = new IntScatterSet();
        for (int i = 0; i < graph.nodeCount(); i++) {
            nodes.add(i);
        }
        return nodes;
    }

    @Test
    public void testSequential() throws Exception {

        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 1, 0)
                .compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());

    }

    @Test
    public void testParallel() throws Exception {

        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 4, 0)
                .compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());
    }

    @Test
    public void testHighCut() throws Exception {

        final MultistepSCC multistep = new MultistepSCC(graph, Pools.DEFAULT, 4, 100_000)
                .compute();

        assertCC(multistep.getConnectedComponents());

        assertEquals(3, multistep.getMaxSetSize());
        assertEquals(3, multistep.getMinSetSize());
        assertEquals(3, multistep.getSetCount());
    }

    private void assertCC(int[] connectedComponents) {
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("a"),
                getMappedNodeId("b"),
                getMappedNodeId("c"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("d"),
                getMappedNodeId("e"),
                getMappedNodeId("f"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("g"),
                getMappedNodeId("h"),
                getMappedNodeId("i"));
    }

    private static void assertBelongSameSet(int[] data, Integer... expected) {
        // check if all belong to same set
        final int needle = data[expected[0]];
        for (int i : expected) {
            assertEquals(needle, data[i]);
        }

        final List<Integer> exp = Arrays.asList(expected);
        // check no other element belongs to this set
        for (int i = 0; i < data.length; i++) {
            if (exp.contains(i)) {
                continue;
            }
            assertNotEquals(needle, data[i]);
        }

    }
}
