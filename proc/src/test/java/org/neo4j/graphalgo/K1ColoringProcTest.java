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
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.helpers.collection.MapUtil;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class K1ColoringProcTest extends ProcTestBase {

    final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(K1ColoringProc.class, GraphLoadProc.class);
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @AllGraphNamesTest
    void testStreaming(String graphImpl) {
        String query = "CALL algo.beta.k1coloring.stream(" +
                       "    null, null, {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, color";

        Map<Long, Long> coloringResult = new HashMap<>(4);
        runQuery(query, MapUtil.map("graph", graphImpl), (row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(nodeId, color);
        });

        assertNotEquals(coloringResult.get(0L), coloringResult.get(1L));
        assertNotEquals(coloringResult.get(0L), coloringResult.get(2L));
    }

    @AllGraphNamesTest
    void testWriting(String graphImpl) {
        String query = "CALL algo.beta.k1coloring(" +
                       "    null, null, {" +
                       "        graph: $graph, write: true, writeProperty: 'color'" +
                       "    }" +
                       ") YIELD nodes, colorCount, didConverge, ranIterations, writeProperty";

        runQuery(query, MapUtil.map("graph", graphImpl), row -> {
            assertEquals(4, row.getNumber("nodes").longValue());
            assertEquals(2, row.getNumber("colorCount").longValue());
            assertEquals("color", row.getString("writeProperty"));
            assertTrue(row.getBoolean("didConverge"));
            assertTrue(row.getNumber("ranIterations").longValue() < 3);
        });

        Map<Long, Long> coloringResult = new HashMap<>(4);
        db.execute("MATCH (n) RETURN id(n) AS id, n.color AS color").accept(row -> {
            long nodeId = row.getNumber("id").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(nodeId, color);
            return true;
        });

        assertNotEquals(coloringResult.get(0L), coloringResult.get(1L));
        assertNotEquals(coloringResult.get(0L), coloringResult.get(2L));
    }

    @Test
    void memrecProc() {
        String query = "CALL algo.beta.k1coloring.memrec(" +
                       "    null, null" +
                       ") YIELD requiredMemory, treeView, bytesMin, bytesMax";
        runQuery(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = MemoryUsage.humanReadable(row.getNumber("bytesMin").longValue());
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
    }

}
