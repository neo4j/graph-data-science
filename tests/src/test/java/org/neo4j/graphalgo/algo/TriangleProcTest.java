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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TriangleProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *      (a)-- (b)--(d)--(e)
 *        \T1/       \T2/
 *        (c)   (g)  (f)
 *          \  /T3\
 *          (h)--(i)
 *
 * @author mknblch
 */
public class TriangleProcTest {

    public static final Label LABEL = Label.label("Node");
    private static GraphDatabaseAPI api;
    private static String[] idToName;

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
                        " (a)-[:TYPE]->(b),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (c)-[:TYPE]->(h),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(f),\n" +
                        " (f)-[:TYPE]->(d),\n" +

                        " (b)-[:TYPE]->(d),\n" +

                        " (g)-[:TYPE]->(h),\n" +
                        " (h)-[:TYPE]->(i),\n" +
                        " (i)-[:TYPE]->(g)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(TriangleProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }

        idToName = new String[9];

        try (Transaction tx = api.beginTx()) {
            for (int i = 0; i < 9; i++) {
                final String name = (String) api.getNodeById(i).getProperty("name");
                idToName[i] = name;
            }
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (api != null) api.shutdown();
    }

    private static int idsum(String... names) {
        int sum = 0;
        for (int i = 0; i < idToName.length; i++) {
            for (String name : names) {
                if (idToName[i].equals(name)) {
                    sum += i;
                }
            }
        }
        return sum;
    }

    @Test
    public void testTriangleCountWriteCypher() throws Exception {
        final String cypher = "CALL algo.triangleCount('Node', '', {concurrency:4, write:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount";
        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
            return true;
        });

        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
        api.execute(request).accept(row -> {
            final int triangles = row.getNumber("t").intValue();
            assertEquals(1, triangles);
            return true;
        });
    }


    @Test
    public void testTriangleCountExp1WriteCypher() throws Exception {
        final String cypher = "CALL algo.triangleCount.forkJoin('Node', '', {concurrency:4, write:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount";
        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
            return true;
        });

        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
        api.execute(request).accept(row -> {
            final int triangles = row.getNumber("t").intValue();
            assertEquals(1, triangles);
            return true;
        });
    }

    @Test
    public void testTriangleCountStream() throws Exception {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
            return true;
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    public void testTriangleCountExp1Stream() throws Exception {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.forkJoin.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
            return true;
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    public void testTriangleStream() throws Exception {
        HashSet<Integer> sums = new HashSet<>();
        final TripleConsumer consumer = (a, b, c) -> sums.add(idsum(a, b, c));
        final String cypher = "CALL algo.triangle.stream('Node', '', {concurrency:4}) YIELD nodeA, nodeB, nodeC";
        api.execute(cypher).accept(row -> {
            final long nodeA = row.getNumber("nodeA").longValue();
            final long nodeB = row.getNumber("nodeB").longValue();
            final long nodeC = row.getNumber("nodeC").longValue();
            consumer.consume(idToName[(int) nodeA], idToName[(int) nodeB], idToName[(int) nodeC]);
            return true;
        });

        assertThat(sums, containsInAnyOrder(0 + 1 + 2, 3 + 4 + 5, 6 + 7 + 8));
    }

    interface TriangleCountConsumer {
        void consume(long nodeId, long triangles);
    }

    interface TripleConsumer {
        void consume(String nodeA, String nodeB, String nodeC);
    }
}
