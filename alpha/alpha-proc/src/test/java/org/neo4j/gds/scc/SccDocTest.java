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
package org.neo4j.gds.scc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SccDocTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE (nAlice:User {name:'Alice'}) " +
        "CREATE (nBridget:User {name:'Bridget'}) " +
        "CREATE (nCharles:User {name:'Charles'}) " +
        "CREATE (nDoug:User {name:'Doug'}) " +
        "CREATE (nMark:User {name:'Mark'}) " +
        "CREATE (nMichael:User {name:'Michael'}) " +
        "CREATE (nAlice)-[:FOLLOW]->(nBridget) " +
        "CREATE (nAlice)-[:FOLLOW]->(nCharles) " +
        "CREATE (nMark)-[:FOLLOW]->(nDoug) " +
        "CREATE (nMark)-[:FOLLOW]->(nMichael) " +
        "CREATE (nBridget)-[:FOLLOW]->(nMichael) " +
        "CREATE (nDoug)-[:FOLLOW]->(nMark) " +
        "CREATE (nMichael)-[:FOLLOW]->(nAlice) " +
        "CREATE (nAlice)-[:FOLLOW]->(nMichael) " +
        "CREATE (nBridget)-[:FOLLOW]->(nAlice) " +
        "CREATE (nMichael)-[:FOLLOW]->(nBridget); ";

    private static final String newLine = System.lineSeparator();

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(SccWriteProc.class, SccStreamProc.class, GraphProjectProc.class);
        registerFunctions(AsNodeFunc.class);

        runQuery("CALL gds.graph.project('graph', 'User', 'FOLLOW')");
    }

    @Test
    void stream1() {
        String query = "CALL gds.alpha.scc.stream('graph', {}) " +
                       "YIELD nodeId, componentId " +
                       "RETURN gds.util.asNode(nodeId).name AS Name, componentId AS Component " +
                       "ORDER BY Component DESC";

        String expected = "+-----------------------+" + newLine +
                          "| Name      | Component |" + newLine +
                          "+-----------------------+" + newLine +
                          "| \"Doug\"    | 3         |" + newLine +
                          "| \"Mark\"    | 3         |" + newLine +
                          "| \"Charles\" | 2         |" + newLine +
                          "| \"Alice\"   | 0         |" + newLine +
                          "| \"Bridget\" | 0         |" + newLine +
                          "| \"Michael\" | 0         |" + newLine +
                          "+-----------------------+" + newLine +
                          "6 rows" + newLine;

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void write1() {
        String query = "CALL gds.alpha.scc.write('graph', { " +
                       "  writeProperty: 'componentId' " +
                       "}) " +
                       "YIELD setCount, maxSetSize, minSetSize; ";

        String expected = "+------------------------------------+" + newLine +
                          "| setCount | maxSetSize | minSetSize |" + newLine +
                          "+------------------------------------+" + newLine +
                          "| 3        | 3          | 1          |" + newLine +
                          "+------------------------------------+" + newLine +
                          "1 row" + newLine;

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void findLargest() {
        String writeQ = "CALL gds.alpha.scc.write('graph', { " +
                       "  writeProperty: 'componentId' " +
                       "}) " +
                       "YIELD setCount, maxSetSize, minSetSize; ";
        runQuery(writeQ);

        String query = "MATCH (u:User) " +
                       "RETURN u.componentId AS Component, count(*) AS PartitionSize " +
                       "ORDER BY PartitionSize DESC " +
                       "LIMIT 1 ";

        String expected = "+---------------------------+" + newLine +
                          "| Component | PartitionSize |" + newLine +
                          "+---------------------------+" + newLine +
                          "| 0         | 3             |" + newLine +
                          "+---------------------------+" + newLine +
                          "1 row" + newLine;

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
