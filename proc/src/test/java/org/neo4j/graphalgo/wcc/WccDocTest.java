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
package org.neo4j.graphalgo.wcc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WccDocTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE (nAlice:User {name: 'Alice'}) " +
        "CREATE (nBridget:User {name: 'Bridget'}) " +
        "CREATE (nCharles:User {name: 'Charles'}) " +
        "CREATE (nDoug:User {name: 'Doug'}) " +
        "CREATE (nMark:User {name: 'Mark'}) " +
        "CREATE (nMichael:User {name: 'Michael'}) " +

        "CREATE (nAlice)-[:LINK {weight: 0.5}]->(nBridget) " +
        "CREATE (nAlice)-[:LINK {weight: 4}]->(nCharles) " +
        "CREATE (nMark)-[:LINK {weight: 1.1}]->(nDoug) " +
        "CREATE (nMark)-[:LINK {weight: 2}]->(nMichael); ";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        runQuery(DB_CYPHER);
        registerProcedures(WccStreamProc.class, WccWriteProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldProduceStreamOutput() {
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        query += " RETURN gds.util.asNode(nodeId).name AS name, componentId" +
                 " ORDER BY componentId, name";

        String expected = "+-------------------------+\n" +
                          "| name      | componentId |\n" +
                          "+-------------------------+\n" +
                          "| \"Alice\"   | 0           |\n" +
                          "| \"Bridget\" | 0           |\n" +
                          "| \"Charles\" | 0           |\n" +
                          "| \"Doug\"    | 3           |\n" +
                          "| \"Mark\"    | 3           |\n" +
                          "| \"Michael\" | 3           |\n" +
                          "+-------------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceWriteOutput() {
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .yields("nodePropertiesWritten", "componentCount");

        String expected = "+----------------------------------------+\n" +
                          "| nodePropertiesWritten | componentCount |\n" +
                          "+----------------------------------------+\n" +
                          "| 6                     | 2              |\n" +
                          "+----------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceWeightedStreamOutput() {
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodeId", "componentId");

        query += " RETURN gds.util.asNode(nodeId).name AS name, componentId" +
                 " ORDER BY componentId, name";

        String expected = "+-------------------------+\n" +
                          "| name      | componentId |\n" +
                          "+-------------------------+\n" +
                          "| \"Alice\"   | 0           |\n" +
                          "| \"Charles\" | 0           |\n" +
                          "| \"Bridget\" | 1           |\n" +
                          "| \"Doug\"    | 3           |\n" +
                          "| \"Mark\"    | 3           |\n" +
                          "| \"Michael\" | 3           |\n" +
                          "+-------------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceWeightedWriteOutput() {
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount");

        String expected = "+----------------------------------------+\n" +
                          "| nodePropertiesWritten | componentCount |\n" +
                          "+----------------------------------------+\n" +
                          "| 6                     | 3              |\n" +
                          "+----------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceSeededStreamOutput() {
        String initQuery = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount");

        String expected = "+----------------------------------------+\n" +
                          "| nodePropertiesWritten | componentCount |\n" +
                          "+----------------------------------------+\n" +
                          "| 6                     | 3              |\n" +
                          "+----------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(initQuery, Result::resultAsString));

        // create new node and relationship
        String dataQuery = "MATCH (b:User {name: 'Bridget'}) " +
                           "CREATE (b)-[:LINK {weight: 2.0}]->(new:User {name: 'Mats'})";
        runQuery(dataQuery);

        // stream with seeding
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withNodeProperty("componentId")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .streamMode()
            .addParameter("seedProperty", "componentId")
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodeId", "componentId");

        query += " RETURN gds.util.asNode(nodeId).name AS name, componentId" +
                 " ORDER BY componentId, name";

        expected = "+-------------------------+\n" +
                   "| name      | componentId |\n" +
                   "+-------------------------+\n" +
                   "| \"Alice\"   | 0           |\n" +
                   "| \"Charles\" | 0           |\n" +
                   "| \"Bridget\" | 1           |\n" +
                   "| \"Mats\"    | 1           |\n" +
                   "| \"Doug\"    | 3           |\n" +
                   "| \"Mark\"    | 3           |\n" +
                   "| \"Michael\" | 3           |\n" +
                   "+-------------------------+\n" +
                   "7 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceSeededWriteOutput() {
        String initQuery = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "componentId")
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount");

        String expected = "+----------------------------------------+\n" +
                          "| nodePropertiesWritten | componentCount |\n" +
                          "+----------------------------------------+\n" +
                          "| 6                     | 3              |\n" +
                          "+----------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(initQuery, Result::resultAsString));

        String dataQuery = "MATCH (b:User {name: 'Bridget'}) " +
                           "CREATE (b)-[:LINK {weight: 2.0}]->(new:User {name: 'Mats'})";

        System.out.println(runQuery(dataQuery, Result::resultAsString));

        // write with seeding
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withNodeProperty("componentId")
            .withRelationshipType("LINK")
            .withRelationshipProperty("weight")
            .algo("wcc")
            .writeMode()
            .addParameter("seedProperty", "componentId")
            .addParameter("writeProperty", "componentId")
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("threshold", 1.0D)
            .yields("nodePropertiesWritten", "componentCount");

        System.out.println(query);

        expected = "+----------------------------------------+\n" +
                   "| nodePropertiesWritten | componentCount |\n" +
                   "+----------------------------------------+\n" +
                   "| 1                     | 3              |\n" +
                   "+----------------------------------------+\n" +
                   "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceStreamOutputOnLoadedGraph() {
        String createGraphQuery = "CALL gds.graph.create('myGraph', ['User'], ['LINK']) YIELD graphName;";
        runQuery(createGraphQuery);

        String query = GdsCypher.call()
            .explicitCreation("myGraph")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        query += " RETURN gds.util.asNode(nodeId).name AS name, componentId" +
                 " ORDER BY componentId, name;";

        String expected = "+-------------------------+\n" +
                          "| name      | componentId |\n" +
                          "+-------------------------+\n" +
                          "| \"Alice\"   | 0           |\n" +
                          "| \"Bridget\" | 0           |\n" +
                          "| \"Charles\" | 0           |\n" +
                          "| \"Doug\"    | 3           |\n" +
                          "| \"Mark\"    | 3           |\n" +
                          "| \"Michael\" | 3           |\n" +
                          "+-------------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceStreamOutputOnCypherProjection() {
        String query = "CALL gds.wcc.stream({" +
                       "   nodeQuery: 'MATCH (u:User) RETURN id(u) AS id', " +
                       "   relationshipQuery: 'MATCH (u1:User)-[:LINK]->(u2:User) RETURN id(u1) AS source, id(u2) AS target'" +
                       "}) " +
                       "YIELD nodeId, componentId " +
                       "RETURN gds.util.asNode(nodeId).name AS name, componentId " +
                       "ORDER BY componentId, name";

        String expected = "+-------------------------+\n" +
                          "| name      | componentId |\n" +
                          "+-------------------------+\n" +
                          "| \"Alice\"   | 0           |\n" +
                          "| \"Bridget\" | 0           |\n" +
                          "| \"Charles\" | 0           |\n" +
                          "| \"Doug\"    | 3           |\n" +
                          "| \"Mark\"    | 3           |\n" +
                          "| \"Michael\" | 3           |\n" +
                          "+-------------------------+\n" +
                          "6 rows\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
