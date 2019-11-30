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
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.compat.MapUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class StronglyConnectedComponentsProcTest extends ProcTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node) " +
            ", (b:Node) " +
            ", (c:Node) " +
            ", (d:Node) " +
            ", (e:Node) " +
            // group 1
            ", (a)-[:TYPE]->(b)" +
            ", (a)<-[:TYPE]-(b)" +
            ", (a)-[:TYPE]->(c)" +
            ", (a)<-[:TYPE]-(c)" +
            ", (b)-[:TYPE]->(c)" +
            ", (b)<-[:TYPE]-(c)" +
            // group 2
            ", (d)-[:TYPE]->(e)" +
            ", (d)<-[:TYPE]-(e)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(GraphLoadProc.class, StronglyConnectedComponentsProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testSccOnLoadedGraph() {
        String graphName = "aliceGraph";
        String loadQuery = "CALL algo.graph.load(" +
                           "    $name, 'Node', 'TYPE', {}" +
                           ")" +
                           "YIELD nodes, relationships";

        runQuery(loadQuery, MapUtil.map("name", graphName));

        String algoQuery = "CALL algo.scc('Node', 'TYPE', {write:true, graph:'" + graphName + "'}) " +
                           "YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize, partitionProperty, writeProperty";

        runQuery(algoQuery, MapUtil.map("name", graphName), row -> {
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
            assertEquals(2, row.getNumber("setCount").longValue());
            assertEquals(2, row.getNumber("minSetSize").longValue());
            assertEquals(3, row.getNumber("maxSetSize").longValue());
            assertEquals("partition", row.getString("partitionProperty"));
            assertEquals("partition", row.getString("writeProperty"));
        });

        runQuery("CALL algo.graph.remove($name)", MapUtil.map("name", graphName));
    }

    @AllGraphNamesTest
    void testScc(String graphName) {
        db
                .execute("CALL algo.scc('Node', 'TYPE', {write:true, graph:'" + graphName + "'}) " +
                         "YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize, partitionProperty, writeProperty")
                .accept(row -> {
                    assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                    assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                    assertEquals(2, row.getNumber("setCount").longValue());
                    assertEquals(2, row.getNumber("minSetSize").longValue());
                    assertEquals(3, row.getNumber("maxSetSize").longValue());
                    assertEquals("partition", row.getString("partitionProperty"));
                    assertEquals("partition", row.getString("writeProperty"));

                    return true;
                });
    }

    @AllGraphNamesTest
    void explicitWriteProperty(String graphName) {
        db
                .execute("CALL algo.scc('Node', 'TYPE', {write:true, graph:'" + graphName + "', writeProperty: 'scc'}) " +
                         "YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize, partitionProperty, writeProperty")
                .accept(row -> {
                    assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                    assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                    assertEquals(2, row.getNumber("setCount").longValue());
                    assertEquals(2, row.getNumber("minSetSize").longValue());
                    assertEquals(3, row.getNumber("maxSetSize").longValue());
                    assertEquals("scc", row.getString("partitionProperty"));
                    assertEquals("scc", row.getString("writeProperty"));

                    return true;
                });
    }
}
