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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
 */
class TriangleProcTest extends ProcTestBase {

    private static String[] idToName;

    @BeforeEach
    void setup() throws Exception {
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

        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(TriangleProc.class);
        runQuery(cypher);

        idToName = new String[9];

        QueryRunner.runInTransaction(db, () -> {
            for (int i = 0; i < 9; i++) {
                final String name = (String) db.getNodeById(i).getProperty("name");
                idToName[i] = name;
            }
        });
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
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
    void testTriangleCountWriteCypher() {
        final String cypher = "CALL algo.triangleCount('Node', '', {concurrency:4, write:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, nodeCount, triangleCount";
        runQuery(cypher, row -> {
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
        });

        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
        runQuery(request, row -> {
            final int triangles = row.getNumber("t").intValue();
            assertEquals(1, triangles);
        });
    }

    @Test
    void testTriangleCountStream() {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        runQuery(cypher, row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    void testTriangleStream() {
        HashSet<Integer> sums = new HashSet<>();
        final TripleConsumer consumer = (a, b, c) -> sums.add(idsum(a, b, c));
        final String cypher = "CALL algo.triangle.stream('Node', '', {concurrency:4}) YIELD nodeA, nodeB, nodeC";
        runQuery(cypher, row -> {
            final long nodeA = row.getNumber("nodeA").longValue();
            final long nodeB = row.getNumber("nodeB").longValue();
            final long nodeC = row.getNumber("nodeC").longValue();
            consumer.consume(idToName[(int) nodeA], idToName[(int) nodeB], idToName[(int) nodeC]);
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
