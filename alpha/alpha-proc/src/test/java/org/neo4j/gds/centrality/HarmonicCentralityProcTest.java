/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.centrality;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HarmonicCentralityProcTest extends BaseProcTest {

    public static final String DB_CYPHER =
        "CREATE (a:Node {name:'a'})" +
        ",      (b:Node {name:'b'})" +
        ",      (c:Node {name:'c'})" +
        ",      (d:Node {name:'d'})" +
        ",      (e:Node {name:'e'})" +
        ",      (a)-[:TYPE]->(b)" +
        ",      (b)-[:TYPE]->(c)" +
        ",      (d)-[:TYPE]->(e)";

    @BeforeEach
    void setupGraph() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(HarmonicCentralityProc.class);
    }

    @Test
    void testStream() {
        var query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness.harmonic")
            .streamMode()
            .yields("nodeId", "centrality");

        var resultMap = new HashMap<Long, Double>();
        runQueryWithRowConsumer(query, row -> {
            resultMap.put(
                row.getNumber("nodeId").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        validateResult(resultMap);
    }

    @Test
    void testWrite() {
        var query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness.harmonic")
            .writeMode()
            .addParameter("writeProperty", "centralityScore")
            .yields();


        runQueryWithRowConsumer(query, row -> {
            assertEquals(5L, row.getNumber("nodes").longValue());
            assertEquals("centralityScore", row.getString("writeProperty"));

            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(0.5, (Double) centralityDistribution.get("max"), 1e-2);
        });

        var resultQuery = "MATCH (n) RETURN id(n) AS id, n.centralityScore AS centrality";

        var resultMap = new HashMap<Long, Double>();
        runQueryWithRowConsumer(resultQuery, row -> {
            resultMap.put(
                row.getNumber("id").longValue(),
                row.getNumber("centrality").doubleValue()
            );
        });

        validateResult(resultMap);
    }

    private void validateResult(Map<Long, Double> resultMap) {
        assertEquals(0.375, resultMap.get(0L), 0.1);
        assertEquals(0.5, resultMap.get(1L), 0.1);
        assertEquals(0.375, resultMap.get(2L), 0.1);
        assertEquals(0.25, resultMap.get(3L), 0.1);
        assertEquals(0.25, resultMap.get(4L), 0.1);
    }
}
