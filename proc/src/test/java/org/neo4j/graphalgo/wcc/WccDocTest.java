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
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WccDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(WccStreamProc.class, WccWriteProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);

        String dbQuery =
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

        String graphCreateQuery = "CALL gds.graph.create(" +
                                   "    'myGraph'," +
                                   "    'User'," +
                                   "    'LINK'," +
                                   "    {" +
                                   "        relationshipProperties: 'weight'" +
                                   "    }" +
                                   ")";

        runQuery(dbQuery);
        runQuery(graphCreateQuery);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldProduceStreamOutput() {
        String query = "CALL gds.wcc.stream('myGraph')\n" +
                       "YIELD nodeId, componentId\n" +
                       "RETURN gds.util.asNode(nodeId).name AS name, componentId ORDER BY componentId, name";

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
        String query = "CALL gds.wcc.write('myGraph', { writeProperty: 'componentId' })\n" +
                       "YIELD nodePropertiesWritten, componentCount";

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
        String query = "CALL gds.wcc.stream('myGraph', { relationshipWeightProperty: 'weight', threshold: 1.0 })\n" +
                       "YIELD nodeId, componentId\n" +
                       "RETURN gds.util.asNode(nodeId).name AS name, componentId ORDER BY componentId, name";

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
        String query = "CALL gds.wcc.write('myGraph', {\n" +
                       "    writeProperty: 'componentId',\n" +
                       "    relationshipWeightProperty: 'weight',\n" +
                       "    threshold: 1.0\n" +
                       "})\n" +
                       "YIELD nodePropertiesWritten, componentCount";

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
        String initQuery = "CALL gds.wcc.write('myGraph', {\n" +
                            "    writeProperty: 'componentId',\n" +
                            "    relationshipWeightProperty: 'weight',\n" +
                            "    threshold: 1.0\n" +
                            "})\n" +
                            "YIELD nodePropertiesWritten, componentCount;";

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

        // create a new in-memory graph
        String graphCreateQuery = "CALL gds.graph.create(\n" +
                                  "    'myGraph-seeded',\n" +
                                  "    'User',\n" +
                                  "    'LINK',\n" +
                                  "    {\n" +
                                  "        nodeProperties: 'componentId',\n" +
                                  "        relationshipProperties: 'weight'\n" +
                                  "    }\n" +
                                  ")";

        runQuery(graphCreateQuery);

        // stream with seeding
        String query = "CALL gds.wcc.stream('myGraph-seeded', {\n" +
                       "    seedProperty: 'componentId',\n" +
                       "    relationshipWeightProperty: 'weight',\n" +
                       "    threshold: 1.0\n" +
                       "})\n" +
                       "YIELD nodeId, componentId\n" +
                       "RETURN gds.util.asNode(nodeId).name AS name, componentId ORDER BY componentId, name";

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
        String initQuery = "CALL gds.wcc.write('myGraph', {\n" +
                           "    writeProperty: 'componentId',\n" +
                           "    relationshipWeightProperty: 'weight',\n" +
                           "    threshold: 1.0\n" +
                           "})\n" +
                           "YIELD nodePropertiesWritten, componentCount;";

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

        // create a new in-memory graph
        String graphCreateQuery = "CALL gds.graph.create(\n" +
                                  "    'myGraph-seeded',\n" +
                                  "    'User',\n" +
                                  "    'LINK',\n" +
                                  "    {\n" +
                                  "        nodeProperties: 'componentId',\n" +
                                  "        relationshipProperties: 'weight'\n" +
                                  "    }\n" +
                                  ")";

        runQuery(graphCreateQuery);

        // write with seeding
        String query = "CALL gds.wcc.write('myGraph-seeded', {\n" +
                       "    seedProperty: 'componentId',\n" +
                       "    writeProperty: 'componentId',\n" +
                       "    relationshipWeightProperty: 'weight',\n" +
                       "    threshold: 1.0\n" +
                       "})\n" +
                       "YIELD nodePropertiesWritten, componentCount";

        expected = "+----------------------------------------+\n" +
                   "| nodePropertiesWritten | componentCount |\n" +
                   "+----------------------------------------+\n" +
                   "| 1                     | 3              |\n" +
                   "+----------------------------------------+\n" +
                   "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
