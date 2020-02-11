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
import org.neo4j.graphalgo.pagerank.PageRankStreamProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphCreateNativeProcDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(LabelPropagationStreamProc.class, PageRankStreamProc.class, GraphCreateProc.class);

        String dbQuery =
            "CREATE " +
            "  (alice:Person)" +
            ", (bob:Person)" +
            ", (sanLeetCisco:City {population: 1337, stateId: 1234})" +
            ", (newOrwellCity:City {population: 1984, stateId: 5678})" +
            ", (sanLeetCisco)-[:ROAD {length: 23, condition: 1.0}]->(newOrwellCity)" +
            ", (sanLeetCisco)-[:ROAD {length: 32}]->(newOrwellCity)" +
            ", (sanLeetCisco)-[:RAIL {length: 42, condition: 0.8}]->(newOrwellCity)";

        runQuery(dbQuery);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void loadSingleNodeLabel() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', {\n" +
                             "        Person: { label: 'Person' }\n" +
                             "    },\n" +
                             "\n" +
                             "    '*'\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 0                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadSingleNodeLabelShorthandSyntax() {
        String createQuery = "CALL gds.graph.create('my-graph', 'Person', '*')\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 0                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleNodeLabels() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', {\n" +
                             "        Person: { label: 'Person' },\n" +
                             "        City: { label: 'City' }\n" +
                             "    },\n" +
                             "    '*'\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 4         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }


    @Test
    void loadMultipleNodeLabelsShorthandSyntax() {
        String createQuery = "CALL gds.graph.create('my-graph', ['Person', 'City'], '*')\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 4         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadAllNodesLabels() {
        String createQuery = "CALL gds.graph.create('my-graph', '*', '*')\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 4         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleNodeProperties() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', {\n" +
                             "        City: {\n" +
                             "            properties: {\n" +
                             "                stateId: {\n" +
                             "                    property: 'stateId'\n" +
                             "                },\n" +
                             "                population: {\n" +
                             "                    property: 'population'\n" +
                             "                }\n" +
                             "            }\n" +
                             "        }\n" +
                             "    },\n" +
                             "\n" +
                             "    '*'\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleNodePropertiesShorthandSyntax() {
        String createQuery = "CALL gds.graph.create('my-graph', 'City', '*', {\n" +
                             "        nodeProperties: ['population', 'stateId']\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleNodePropertiesWithRenaming() {
        String createQuery = "CALL gds.graph.create('my-graph', 'City', '*', {\n" +
                             "        nodeProperties: ['population', { community: 'stateId' }]\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadNodePropertiesAndRunLPA() {
        String createQuery = "CALL gds.graph.create('my-graph', 'City', '*', {\n" +
                             "        nodeProperties: ['population', { community: 'stateId' }]\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.labelPropagation.stream(\n" +
                           "    'my-graph', {\n" +
                           "        seedProperty: 'community'\n" +
                           "    }\n" +
                           ") YIELD nodeId, communityId;";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 2      | 5678        |\n" +
                   "| 3      | 5678        |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipTypes() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph',\n" +
                             "    'City',\n" +
                             "    {\n" +
                             "        ROAD: {\n" +
                             "            type: 'ROAD',\n" +
                             "            projection: 'NATURAL'\n" +
                             "        },\n" +
                             "        RAIL: {\n" +
                             "            type: 'RAIL',\n" +
                             "            projection: 'NATURAL'\n" +
                             "        }\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipTypesShorthandSyntaxOption() {
        String createQuery = "CALL gds.graph.create('my-graph', 'City', ['ROAD', 'RAIL'])\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipTypesAndRunPageRank() {
        String createQuery = "CALL gds.graph.create( 'my-graph', 'City', ['ROAD', 'RAIL'])\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 3                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.pageRank.stream('my-graph', { relationshipTypes: ['ROAD'] }) YIELD nodeId, score;";

        expected = "+------------------------------+\n" +
                   "| nodeId | score               |\n" +
                   "+------------------------------+\n" +
                   "| 2      | 0.15000000000000002 |\n" +
                   "| 3      | 0.27750000506639483 |\n" +
                   "+------------------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));

        algoQuery = "CALL gds.pageRank.stream('my-graph', { relationshipTypes: ['RAIL'] }) YIELD nodeId, score;";

        expected = "+------------------------------+\n" +
                   "| nodeId | score               |\n" +
                   "+------------------------------+\n" +
                   "| 2      | 0.15000000000000002 |\n" +
                   "| 3      | 0.27750000506639483 |\n" +
                   "+------------------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipProperties() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', {\n" +
                             "        City: {\n" +
                             "            properties: {\n" +
                             "                community: {\n" +
                             "                    property: 'stateId'\n" +
                             "                }\n" +
                             "            }\n" +
                             "        }\n" +
                             "    }, {\n" +
                             "        ROAD: {\n" +
                             "            properties: {\n" +
                             "                quality: {\n" +
                             "                    property: 'condition'\n" +
                             "                },\n" +
                             "                distance: {\n" +
                             "                    property: 'length'\n" +
                             "                }\n" +
                             "            }\n" +
                             "        }\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 2                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipPropertiesShorthandSyntax() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', 'City', 'ROAD', {\n" +
                             "        nodeProperties: { community: 'stateId' },\n" +
                             "        relationshipProperties: [{ quality: 'condition' }, { distance: 'length' }]\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 2                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadMultipleRelationshipPropertiesAndRunLPA() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', 'City', 'ROAD', {\n" +
                             "        nodeProperties: { community: 'stateId' },\n" +
                             "        relationshipProperties: [{ quality: 'condition' }, { distance: 'length' }]\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 2                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.labelPropagation.stream(\n" +
                           "    'my-graph', {\n" +
                           "        seedProperty: 'community',\n" +
                           "        relationshipWeightProperty: 'quality'\n" +
                           "    }\n" +
                           ") YIELD nodeId, communityId;";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 2      | 1234        |\n" +
                   "| 3      | 5678        |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));

        algoQuery = "CALL gds.labelPropagation.stream(\n" +
                    "    'my-graph', {\n" +
                    "        seedProperty: 'community',\n" +
                    "        relationshipWeightProperty: 'distance'\n" +
                    "    }\n" +
                    ") YIELD nodeId, communityId;";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 2      | 5678        |\n" +
                   "| 3      | 5678        |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }

    @Test
    void loadRelationshipsWithAggregation() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', {\n" +
                             "        City: {\n" +
                             "            properties: {\n" +
                             "                community: {\n" +
                             "                    property: 'stateId'\n" +
                             "                }\n" +
                             "            }\n" +
                             "        }\n" +
                             "    }, {\n" +
                             "        ROAD: {\n" +
                             "            properties: {\n" +
                             "                maxQuality: {\n" +
                             "                    property: 'condition',\n" +
                             "                    aggregation: 'MAX',\n" +
                             "                    defaultValue: 1.0\n" +
                             "                }\n" +
                             "            }\n" +
                             "        }\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 1                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadRelationshipsWithAggregationShorthandSyntax() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', 'City', 'ROAD', {\n" +
                             "        nodeProperties: { community: 'stateId' },\n" +
                             "        relationshipProperties: { maxQuality: { property: 'condition', aggregation: 'MAX', defaultValue: 1.0 }}\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 1                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));
    }

    @Test
    void loadRelationshipsWithAggregationAndRunLPA() {
        String createQuery = "CALL gds.graph.create(\n" +
                             "    'my-graph', 'City', 'ROAD', {\n" +
                             "        nodeProperties: { community: 'stateId' },\n" +
                             "        relationshipProperties: { maxQuality: { property: 'condition', aggregation: 'MAX', defaultValue: 1.0 }}\n" +
                             "    }\n" +
                             ")\n" +
                             "YIELD graphName, nodeCount, relationshipCount;";

        String expected = "+--------------------------------------------+\n" +
                          "| graphName  | nodeCount | relationshipCount |\n" +
                          "+--------------------------------------------+\n" +
                          "| \"my-graph\" | 2         | 1                 |\n" +
                          "+--------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(createQuery, Result::resultAsString));

        String algoQuery = "CALL gds.labelPropagation.stream(\n" +
                           "    'my-graph', {\n" +
                           "        seedProperty: 'community',\n" +
                           "        relationshipWeightProperty: 'maxQuality'\n" +
                           "    }\n" +
                           ") YIELD nodeId, communityId;";

        expected = "+----------------------+\n" +
                   "| nodeId | communityId |\n" +
                   "+----------------------+\n" +
                   "| 2      | 5678        |\n" +
                   "| 3      | 5678        |\n" +
                   "+----------------------+\n" +
                   "2 rows\n";

        assertEquals(expected, runQuery(algoQuery, Result::resultAsString));
    }




}
