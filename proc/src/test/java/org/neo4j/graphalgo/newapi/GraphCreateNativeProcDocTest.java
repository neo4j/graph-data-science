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

class GraphCreateNativeProcDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(LabelPropagationStreamProc.class, GraphCreateProc.class);

        String dbQuery =
            "CREATE " +
            "  (alice:Person)" +
            ", (bob:Person)" +
            ", (sanLeetCisco:City {population: 1337, stateId: 1234})" +
            ", (newOrwellCity:City {population: 1984, stateId: 5678})" +
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
    void loadSingleNodeLabel() {
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
        String createQuery = "CALL gds.graph.create('my-graph', 'Person | City', '*')\n" +
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
    void loadSingleNodeProperty() {
        String createQuery = "CALL gds.graph.create('my-graph', 'City', '*', {\n" +
                             "        nodeProperties: 'population'\n" +
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
    void loadMultipleNodeProperties() {
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
}
