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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilityDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class, VersionFunc.class);

        String dbQuery =
            "CREATE (nAlice:User {name: 'Alice'}) " +
            "CREATE (nBridget:User {name: 'Bridget'}) " +
            "CREATE (nCharles:User {name: 'Charles'}) " +

            "CREATE (nAlice)-[:LINK]->(nBridget) " +
            "CREATE (nBridget)-[:LINK]->(nCharles) ";

        String graphCreateQuery = "CALL gds.graph.create(" +
                                   "    'myGraph'," +
                                   "    'User'," +
                                   "    'LINK'," +
                                   "    {}" +
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
    void shouldPrintCorrectVersion() {
        String query = "RETURN gds.version() as version";

        String expected = "+---------+\n" +
                          "| version |\n" +
                          "+---------+\n" +
                          "| \"0.9.1\" |\n" +
                          "+---------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldReturnANode() {
        String query = " MATCH (u:User{name: 'Alice'})" +
                       " WITH id(u) as nodeId" +
                       " RETURN gds.util.asNode(nodeId) as node";

        String expected = "+-----------------------+\n" +
                          "| node                  |\n" +
                          "+-----------------------+\n" +
                          "| Node[0]{name:\"Alice\"} |\n" +
                          "+-----------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldReturnNodes() {
        String query = " MATCH (u:User)" +
                       " WHERE NOT u.name = 'Charles'" +
                       " WITH collect(id(u)) as nodeIds" +
                       " RETURN gds.util.asNodes(nodeIds) as nodes";

        String expected = "+-------------------------------------------------+\n" +
                          "| nodes                                           |\n" +
                          "+-------------------------------------------------+\n" +
                          "| [Node[0]{name:\"Alice\"},Node[1]{name:\"Bridget\"}] |\n" +
                          "+-------------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, runQuery(query, Result::resultAsString));
    }
}
