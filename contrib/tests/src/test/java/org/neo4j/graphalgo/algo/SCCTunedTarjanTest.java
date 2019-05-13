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
package org.neo4j.graphalgo.algo;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.ConnectedComponentsTest;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.impl.scc.SCCTunedTarjan;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

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
public class SCCTunedTarjanTest extends ConnectedComponentsTest {

    protected SCCTunedTarjanTest(final Class<? extends GraphFactory> graphImpl) {
        super(graphImpl);
    }

    @BeforeClass
    public static void setup() throws KernelException {
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

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(StronglyConnectedComponentsProc.class);

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
    public static void shutdownGraph() throws Exception {
        if (api != null) api.shutdown();
    }

    @Test
    public void testDirect() throws Exception {

        final SCCTunedTarjan tarjan = new SCCTunedTarjan(graph)
                .compute();

        assertCC(tarjan.getConnectedComponents());

        assertEquals(3, tarjan.getMaxSetSize());
        assertEquals(3, tarjan.getMinSetSize());
        assertEquals(3, tarjan.getSetCount());
    }

    @Test
    public void testCypher() throws Exception {

        String cypher = "CALL algo.scc.recursive.tunedTarjan('', '', {write:true}) YIELD loadMillis, computeMillis, writeMillis";

        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            return true;
        });

        String cypher2 = "MATCH (n) RETURN n.partition as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        api.execute(cypher2).accept(row -> {
            testMap.addTo(row.getNumber("c").intValue(), 1);
            return true;
        });

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }


    @Test
    public void testCypherStream() throws Exception {

        final IntIntScatterMap testMap = new IntIntScatterMap();

        String cypher = "CALL algo.scc.recursive.tunedTarjan.stream() YIELD nodeId, partition";

        api.execute(cypher).accept(row -> {
            testMap.addTo(row.getNumber("partition").intValue(), 1);
            return true;
        });

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }
}
