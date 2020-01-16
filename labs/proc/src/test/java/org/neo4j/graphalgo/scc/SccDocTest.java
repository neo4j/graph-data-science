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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.scc.SccProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

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

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        runQuery(DB_CYPHER);
        registerProcedures(SccProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void stream1() {
        String query = "CALL gds.alpha.scc.stream({ " +
                       "  nodeProjection: 'User', " +
                       "  relationshipProjection: 'FOLLOW' " +
                       "}) " +
                       "YIELD nodeId, partition " +
                       "RETURN gds.util.asNode(nodeId).name AS Name, partition AS Partition " +
                       "ORDER BY partition DESC";

        String expected = "+-----------------------+\n" +
                          "| Name      | Partition |\n" +
                          "+-----------------------+\n" +
                          "| \"Doug\"    | 3         |\n" +
                          "| \"Mark\"    | 3         |\n" +
                          "| \"Charles\" | 2         |\n" +
                          "| \"Alice\"   | 0         |\n" +
                          "| \"Bridget\" | 0         |\n" +
                          "| \"Michael\" | 0         |\n" +
                          "+-----------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void write1() {
        String query = "CALL gds.alpha.scc.write({ " +
                       "  nodeProjection: 'User', " +
                       "  relationshipProjection: 'FOLLOW', " +
                       "  writeProperty: 'partition' " +
                       "}) " +
                       "YIELD setCount, maxSetSize, minSetSize; ";

        String expected = "+------------------------------------+\n" +
                          "| setCount | maxSetSize | minSetSize |\n" +
                          "+------------------------------------+\n" +
                          "| 3        | 3          | 1          |\n" +
                          "+------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void findLargest() {
        String writeQ = "CALL gds.alpha.scc.write({ " +
                       "  nodeProjection: 'User', " +
                       "  relationshipProjection: 'FOLLOW', " +
                       "  writeProperty: 'partition' " +
                       "}) " +
                       "YIELD setCount, maxSetSize, minSetSize; ";
        runQuery(writeQ);

        String query = "MATCH (u:User) " +
                       "RETURN u.partition AS Partition, count(*) AS PartitionSize " +
                       "ORDER BY PartitionSize DESC " +
                       "LIMIT 1 ";

        String expected = "+---------------------------+\n" +
                          "| Partition | PartitionSize |\n" +
                          "+---------------------------+\n" +
                          "| 0         | 3             |\n" +
                          "+---------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void cypherProjection() {
        String query = "CALL gds.alpha.scc.stream({ " +
                       "  nodeQuery: 'MATCH (u:User) RETURN id(u) AS id', " +
                       "  relationshipQuery: 'MATCH (u1:User)-[:FOLLOW]->(u2:User) RETURN id(u1) AS source, id(u2) AS target' " +
                       "}) " +
                       "YIELD nodeId, partition " +
                       "RETURN gds.util.asNode(nodeId).name AS Name, partition AS Partition " +
                       "ORDER BY partition DESC ";
        System.out.println(query);

        String expected = "+-----------------------+\n" +
                          "| Name      | Partition |\n" +
                          "+-----------------------+\n" +
                          "| \"Doug\"    | 3         |\n" +
                          "| \"Mark\"    | 3         |\n" +
                          "| \"Charles\" | 2         |\n" +
                          "| \"Alice\"   | 0         |\n" +
                          "| \"Bridget\" | 0         |\n" +
                          "| \"Michael\" | 0         |\n" +
                          "+-----------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
