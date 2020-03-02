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

package org.neo4j.graphalgo.centrality.eigenvector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EigenvectorCentralityDocTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();

        final String cypherUnweighted = "CREATE (home:Page {name:'Home'})" +
                                        ", (about:Page {name:'About'})" +
                                        ", (product:Page {name:'Product'})" +
                                        ", (links:Page {name:'Links'})" +
                                        ", (a:Page {name:'Site A'})" +
                                        ", (b:Page {name:'Site B'})" +
                                        ", (c:Page {name:'Site C'})" +
                                        ", (d:Page {name:'Site D'})" +
                                        ", (home)-[:LINKS]->(about)" +
                                        ", (about)-[:LINKS]->(home)" +
                                        ", (product)-[:LINKS]->(home)" +
                                        ", (home)-[:LINKS]->(product)" +
                                        ", (links)-[:LINKS]->(home)" +
                                        ", (home)-[:LINKS]->(links)" +
                                        ", (links)-[:LINKS]->(a)" +
                                        ", (a)-[:LINKS]->(home)" +
                                        ", (links)-[:LINKS]->(b)" +
                                        ", (b)-[:LINKS]->(home)" +
                                        ", (links)-[:LINKS]->(c)" +
                                        ", (c)-[:LINKS]->(home)" +
                                        ", (links)-[:LINKS]->(d)" +
                                        ", (d)-[:LINKS]->(home)";

        registerProcedures(EigenvectorCentralityProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(cypherUnweighted);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldStream() {
        String query = "CALL gds.alpha.eigenvector.stream({" +
                       "  nodeProjection: 'Page', " +
                       "  relationshipProjection: 'LINKS'" +
                       "}) " +
                       "YIELD nodeId, score " +
                       "RETURN gds.util.asNode(nodeId).name AS page,score " +
                       "ORDER BY score DESC";

        String actual = runQuery(query, Result::resultAsString);

        String expected = "+--------------------------------+\n" +
                          "| page      | score              |\n" +
                          "+--------------------------------+\n" +
                          "| \"Home\"    | 31.458663403987885 |\n" +
                          "| \"About\"   | 14.403928011655807 |\n" +
                          "| \"Product\" | 14.403928011655807 |\n" +
                          "| \"Links\"   | 14.403928011655807 |\n" +
                          "| \"Site A\"  | 6.572431668639183  |\n" +
                          "| \"Site B\"  | 6.572431668639183  |\n" +
                          "| \"Site C\"  | 6.572431668639183  |\n" +
                          "| \"Site D\"  | 6.572431668639183  |\n" +
                          "+--------------------------------+\n" +
                          "8 rows\n";

        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String query = "CALL gds.alpha.eigenvector.write({" +
                       "  nodeProjection: 'Page'," +
                       "  relationshipProjection: 'LINKS'," +
                       "  writeProperty: 'eigenvector'" +
                       "}) " +
                       "YIELD nodes, iterations, dampingFactor, writeProperty";
        String actual = runQuery(query, Result::resultAsString);

        String expected = "+----------------------------------------------------+\n" +
                          "| nodes | iterations | dampingFactor | writeProperty |\n" +
                          "+----------------------------------------------------+\n" +
                          "| 0     | 20         | 1.0           | \"eigenvector\" |\n" +
                          "+----------------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, actual);
    }

    @Test
    void shouldStreamWithMaxNormalization() {
        String query = "CALL gds.alpha.eigenvector.stream({" +
                       "  nodeProjection: 'Page', " +
                       "  relationshipProjection: 'LINKS', " +
                       "  normalization: 'max'" +
                       "}) " +
                       "YIELD nodeId, score " +
                       "RETURN gds.util.asNode(nodeId).name AS page,score " +
                       "ORDER BY score DESC";

        String actual = runQuery(query, Result::resultAsString);

        String expected = "+---------------------------------+\n" +
                          "| page      | score               |\n" +
                          "+---------------------------------+\n" +
                          "| \"Home\"    | 1.0                 |\n" +
                          "| \"About\"   | 0.4578684042192931  |\n" +
                          "| \"Product\" | 0.4578684042192931  |\n" +
                          "| \"Links\"   | 0.4578684042192931  |\n" +
                          "| \"Site A\"  | 0.20892278811203477 |\n" +
                          "| \"Site B\"  | 0.20892278811203477 |\n" +
                          "| \"Site C\"  | 0.20892278811203477 |\n" +
                          "| \"Site D\"  | 0.20892278811203477 |\n" +
                          "+---------------------------------+\n" +
                          "8 rows\n";

        assertEquals(expected, actual);
    }

    @Test
    void shouldLoadWithCypher() {
        String query = "CALL gds.alpha.eigenvector.write({" +
                       "  nodeQuery: 'MATCH (p:Page) RETURN id(p) as id'," +
                       "  relationshipQuery: 'MATCH (p1:Page)-[:LINKS]->(p2:Page) RETURN id(p1) as source, id(p2) as target'," +
                       "  maxIterations: 5" +
                       "}) " +
                       "YIELD nodes, iterations, dampingFactor, writeProperty";
        String actual = runQuery(query, Result::resultAsString);

        String expected = "+----------------------------------------------------+\n" +
                          "| nodes | iterations | dampingFactor | writeProperty |\n" +
                          "+----------------------------------------------------+\n" +
                          "| 0     | 5          | 1.0           | \"eigenvector\" |\n" +
                          "+----------------------------------------------------+\n" +
                          "1 row\n";

        assertEquals(expected, actual);
    }

}
