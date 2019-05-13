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

import com.carrotsearch.hppc.IntScatterSet;
import com.carrotsearch.hppc.IntSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.ConnectedComponentsTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.multistepscc.MultistepSCC;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;

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
public class MultistepSCCTest extends ConnectedComponentsTest {


    private static GraphDatabaseAPI api;

    private static Graph graph;

    public MultistepSCCTest(Class<? extends GraphFactory> graphImpl, String name) {
        super(graphImpl);
        graph = new GraphLoader(api)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(graphImpl);
    }

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
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (api != null) api.shutdown();
        graph = null;
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

}
