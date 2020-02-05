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
package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphCreateCypherProcDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(LabelPropagationStreamProc.class, GraphCreateProc.class);

        String dbQuery =
            "CREATE " +
            "  (sanLeetCisco:City {population: 1337, zipCode: 1234})" +
            ", (newOrwellCity:City {population: 1984, zipCode: 5678})" +
            ", (sanLeetCisco)-[:ROAD {distance: 23, quality: 1.0}]->(newOrwellCity)" +
            ", (sanLeetCisco)-[:ROAD {distance: 32}]->(newOrwellCity)" +
            ", (sanLeetCisco)-[:RAIL {distance: 42, quality: 0.8}]->(newOrwellCity)";

        runQuery(dbQuery);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void loadAllExample() {
        String createQuery = "CALL gds.graph.create.cypher(\n" +
                             "    'myCypherGraph',\n" +
                             "    'MATCH (n) RETURN id(n) AS id',\n" +
                             "    'MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target'\n" +
                             ") YIELD nodeCount, relationshipCount";

        String expected = "+-------------------------------+\n" +
                          "| nodeCount | relationshipCount |\n" +
                          "+-------------------------------+\n" +
                          "| 2         | 3                 |\n" +
                          "+-------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadPropertiesAndRunLPA() {
        String createQuery = "CALL gds.graph.create.cypher(\n" +
                             "    'myCypherGraph',\n" +
                             "    'MATCH (n:City) RETURN id(n) AS id, n.zipCode AS community, n.population AS population',\n" +
                             "    'MATCH (n:City)-[r:ROAD]->(m:City) RETURN id(n) AS source, id(m) AS target, r.distance AS distance, coalesce(r.condition, 0.5) AS quality'\n" +
                             ") YIELD nodeCount, relationshipCount";

        String expected = "+-------------------------------+\n" +
                          "| nodeCount | relationshipCount |\n" +
                          "+-------------------------------+\n" +
                          "| 2         | 2                 |\n" +
                          "+-------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.labelPropagation.stream(\n" +
                           "    'myCypherGraph', {\n" +
                           "        seedProperty: 'community',\n" +
                           "        relationshipWeightProperty: 'quality'\n" +
                           "    }\n" +
                           ")";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 0      | 5678        |\n" +
                   "| 1      | 5678        |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }

    @Test
    void loadRelationshipTypesAndRunLPA() {
        String createQuery = "CALL gds.graph.create.cypher(\n" +
                             "    'myCypherGraph',\n" +
                             "    'MATCH (n:City) RETURN id(n) AS id',\n" +
                             "    'MATCH (n:City)-[r:ROAD|RAIL]->(m:City) RETURN id(n) AS source, id(m) AS target, type(r) AS type'\n" +
                             ") YIELD nodeCount, relationshipCount";

        String expected = "+-------------------------------+\n" +
                          "| nodeCount | relationshipCount |\n" +
                          "+-------------------------------+\n" +
                          "| 2         | 3                 |\n" +
                          "+-------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.labelPropagation.stream(\n" +
                           "    'myCypherGraph', {\n" +
                           "        relationshipTypes: ['ROAD']\n" +
                           "    }\n" +
                           ")";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 0      | 1           |\n" +
                   "| 1      | 1           |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }

    @Test
    void loadWithAggregation() {
        String createQuery = "CALL gds.graph.create.cypher(\n" +
                             "    'myCypherGraph',\n" +
                             "    'MATCH (n:City) RETURN id(n) AS id, n.zipCode AS community, n.population AS population',\n" +
                             "    'MATCH (n:City)-[r:ROAD]->(m:City) RETURN id(n) AS source, id(m) AS target, r.distance AS distance, r.condition AS quality',\n" +
                             "    {\n" +
                             "        relationshipProperties: {\n" +
                             "            minDistance: {\n" +
                             "                property: 'distance',\n" +
                             "                aggregation: 'MIN',\n" +
                             "                defaultValue: 42.0\n" +
                             "            },\n" +
                             "            maxQuality: {\n" +
                             "                property: 'quality',\n" +
                             "                aggregation: 'MAX',\n" +
                             "                defaultValue: 1.0\n" +
                             "            }\n" +
                             "        }\n" +
                             "    }\n" +
                             ") YIELD nodeCount, relationshipCount";

        String expected = "+-------------------------------+\n" +
                          "| nodeCount | relationshipCount |\n" +
                          "+-------------------------------+\n" +
                          "| 2         | 1                 |\n" +
                          "+-------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }
}
