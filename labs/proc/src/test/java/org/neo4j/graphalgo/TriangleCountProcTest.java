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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
class TriangleCountProcTest extends BaseProcTest {

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
        registerProcedures(TriangleProc.class, ModernTriangleProc.class, ModernTriangleCountProc.class);
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

    @Test
    void testTriangleCountStream() {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles";
        runQueryWithRowConsumer(cypher, row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

    @Test
    void testStreaming() {
        TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds", "alpha", "triangleCount")
            .streamMode()
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long triangles = row.getNumber("triangles").longValue();
            mock.consume(nodeId, triangles);
        });
        verify(mock, times(9)).consume(anyLong(), eq(1L));
    }

//    @Test
//    void testWriting() {
//        String query = GdsCypher.call()
//            .loadEverything(Projection.UNDIRECTED)
//            .algo("gds", "alpha", "triangleCount")
//            .writeMode()
//            .yields();
//
//        runQueryWithRowConsumer(query, row -> {
//            final long loadMillis = row.getNumber("loadMillis").longValue();
//            final long computeMillis = row.getNumber("computeMillis").longValue();
//            final long writeMillis = row.getNumber("writeMillis").longValue();
//            final long nodeCount = row.getNumber("nodeCount").longValue();
//            final long triangleCount = row.getNumber("triangleCount").longValue();
//            assertNotEquals(-1, loadMillis);
//            assertNotEquals(-1, computeMillis);
//            assertNotEquals(-1, writeMillis);
//            assertEquals(3, triangleCount);
//            assertEquals(9, nodeCount);
//        });
//
//        final String request = "MATCH (n) WHERE exists(n.triangles) RETURN n.triangles as t";
//        runQueryWithRowConsumer(request, row -> {
//            final int triangles = row.getNumber("t").intValue();
//            assertEquals(1, triangles);
//        });
//    }

    interface TriangleCountConsumer {
        void consume(long nodeId, long triangles);
    }
}
