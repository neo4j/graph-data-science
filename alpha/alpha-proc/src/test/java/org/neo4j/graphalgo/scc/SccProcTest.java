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
package org.neo4j.graphalgo.scc;

import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SccProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (f:Node {name: 'f'})" +
        ", (g:Node {name: 'g'})" +
        ", (h:Node {name: 'h'})" +
        ", (i:Node {name: 'i'})" +

        ", (a)-[:TYPE {cost: 5}]->(b)" +
        ", (b)-[:TYPE {cost: 5}]->(c)" +
        ", (c)-[:TYPE {cost: 5}]->(a)" +

        ", (d)-[:TYPE {cost: 2}]->(e)" +
        ", (e)-[:TYPE {cost: 2}]->(f)" +
        ", (f)-[:TYPE {cost: 2}]->(d)" +

        ", (a)-[:TYPE {cost: 2}]->(d)" +

        ", (g)-[:TYPE {cost: 3}]->(h)" +
        ", (h)-[:TYPE {cost: 3}]->(i)" +
        ", (i)-[:TYPE {cost: 3}]->(g)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(SccProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testWriteWithDefaultWriteProperty() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.scc")
            .writeMode()
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long createMillis = row.getNumber("createMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            assertEquals(3, row.getNumber("setCount").longValue());
            assertEquals(3, row.getNumber("minSetSize").longValue());
            assertEquals(3, row.getNumber("maxSetSize").longValue());
            assertEquals("partition", row.getString("writeProperty"));
            assertNotEquals(-1, createMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
        });

        String validationQuery = "MATCH (n) RETURN n.partition as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQueryWithRowConsumer(validationQuery, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

    @Test
    void testWriteWithExplicitWriteProperty() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.scc")
            .writeMode()
            .addParameter("writeProperty", "scc")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long createMillis = row.getNumber("createMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            assertEquals(3, row.getNumber("setCount").longValue());
            assertEquals(3, row.getNumber("minSetSize").longValue());
            assertEquals(3, row.getNumber("maxSetSize").longValue());
            assertEquals("scc", row.getString("writeProperty"));
            assertNotEquals(-1, createMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
        });

        String validationQuery = "MATCH (n) RETURN n.scc as c";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        runQueryWithRowConsumer(validationQuery, row -> testMap.addTo(row.getNumber("c").intValue(), 1));

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

    @Test
    void testStream() {
        final IntIntScatterMap testMap = new IntIntScatterMap();

        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.scc")
            .streamMode()
            .yields();

        runQueryWithRowConsumer(query, row ->
            testMap.addTo(row.getNumber("partition").intValue(), 1)
        );

        // 3 sets with 3 elements each
        assertEquals(3, testMap.size());
        for (IntIntCursor cursor : testMap) {
            assertEquals(3, cursor.value);
        }
    }

}
