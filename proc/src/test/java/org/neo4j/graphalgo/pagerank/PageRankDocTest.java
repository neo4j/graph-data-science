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
package org.neo4j.graphalgo.pagerank;

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
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PageRankDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
                builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(PageRankStreamProc.class, PageRankWriteProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);

        String dbQuery = "CREATE (home:Page {name:'Home'})" +
                          "CREATE (about:Page {name:'About'})" +
                          "CREATE (product:Page {name:'Product'})" +
                          "CREATE (links:Page {name:'Links'})" +
                          "CREATE (a:Page {name:'Site A'})" +
                          "CREATE (b:Page {name:'Site B'})" +
                          "CREATE (c:Page {name:'Site C'})" +
                          "CREATE (d:Page {name:'Site D'})" +

                          "CREATE (home)-[:LINKS {weight: 0.2}]->(about)" +
                          "CREATE (home)-[:LINKS {weight: 0.2}]->(links)" +
                          "CREATE (home)-[:LINKS {weight: 0.6}]->(product)" +
                          "CREATE (about)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (product)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (a)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (b)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (c)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (d)-[:LINKS {weight: 1.0}]->(home)" +
                          "CREATE (links)-[:LINKS {weight: 0.8}]->(home)" +
                          "CREATE (links)-[:LINKS {weight: 0.05}]->(a)" +
                          "CREATE (links)-[:LINKS {weight: 0.05}]->(b)" +
                          "CREATE (links)-[:LINKS {weight: 0.05}]->(c)" +
                          "CREATE (links)-[:LINKS {weight: 0.05}]->(d)";

        String graphCreateQuery = "CALL gds.graph.create(" +
                                   "    'myGraph'," +
                                   "    'Page'," +
                                   "    'LINKS'," +
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

    // Queries and results match pagerank.adoc unweighted example section; should read from there in a future
    // Doesn't have any assertions; those should be to verify results with contents in pagerank.adoc
    // This is left for a future task
    @Test
    void unweighted() {
        String q1 =
                "CALL gds.pageRank.stream('myGraph', {\n" +
                "  maxIterations: 20,\n" +
                "  dampingFactor: 0.85\n" +
                "})\n" +
                "YIELD nodeId, score\n" +
                "RETURN gds.util.asNode(nodeId).name AS name, score\n" +
                "ORDER BY score DESC";

        String expectedString = "+--------------------------------+\n" +
                                "| name      | score              |\n" +
                                "+--------------------------------+\n" +
                                "| \"Home\"    | 3.2362017153762284 |\n" +
                                "| \"About\"   | 1.0611098567023873 |\n" +
                                "| \"Product\" | 1.0611098567023873 |\n" +
                                "| \"Links\"   | 1.0611098567023873 |\n" +
                                "| \"Site A\"  | 0.3292259009438567 |\n" +
                                "| \"Site B\"  | 0.3292259009438567 |\n" +
                                "| \"Site C\"  | 0.3292259009438567 |\n" +
                                "| \"Site D\"  | 0.3292259009438567 |\n" +
                                "+--------------------------------+\n" +
                                "8 rows\n";

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        String q2 =
            "CALL gds.pageRank.write('myGraph', {\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85,\n" +
            "  writeProperty: 'pagerank'\n" +
            "})\n" +
            "YIELD nodePropertiesWritten AS writtenProperties, ranIterations";

        String r2 = runQuery(q2, Result::resultAsString);

        expectedString = "+-----------------------------------+\n" +
                         "| writtenProperties | ranIterations |\n" +
                         "+-----------------------------------+\n" +
                         "| 8                 | 20            |\n" +
                         "+-----------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, r2);
    }

    // Queries and results match pagerank.adoc weighted example section
    // used to test that the results are correct in the docs
    @Test
    void weighted() {
        String q1 =
            "CALL gds.pageRank.stream('myGraph', {\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85,\n" +
            "  relationshipWeightProperty: 'weight'\n" +
            "})\n" +
            "YIELD nodeId, score " +
            "RETURN gds.util.asNode(nodeId).name AS name, score " +
            "ORDER BY score DESC ";

        String expectedString = "+---------------------------------+\n" +
                                "| name      | score               |\n" +
                                "+---------------------------------+\n" +
                                "| \"Home\"    | 3.5528567278757683  |\n" +
                                "| \"Product\" | 1.9541301048360766  |\n" +
                                "| \"About\"   | 0.7513767024036497  |\n" +
                                "| \"Links\"   | 0.7513767024036497  |\n" +
                                "| \"Site A\"  | 0.18167360233856014 |\n" +
                                "| \"Site B\"  | 0.18167360233856014 |\n" +
                                "| \"Site C\"  | 0.18167360233856014 |\n" +
                                "| \"Site D\"  | 0.18167360233856014 |\n" +
                                "+---------------------------------+\n" +
                                "8 rows\n";

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));


        String q2 =
            "CALL gds.pageRank.write('myGraph', {\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85,\n" +
            "  writeProperty: 'pagerank',\n" +
            "  relationshipWeightProperty: 'weight'\n" +
            "})\n" +
            "YIELD nodePropertiesWritten AS writtenProperties, ranIterations";

        expectedString = "+-----------------------------------+\n" +
                         "| writtenProperties | ranIterations |\n" +
                         "+-----------------------------------+\n" +
                         "| 8                 | 20            |\n" +
                         "+-----------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }

    @Test
    void personalized() {
        String q1 =
            "MATCH (siteA:Page {name: 'Site A'})\n" +
            "CALL gds.pageRank.stream('myGraph', {\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85,\n" +
            "  sourceNodes: [siteA]\n" +
            "})\n" +
            "YIELD nodeId, score\n" +
            "RETURN gds.util.asNode(nodeId).name AS name, score\n" +
            "ORDER BY score DESC ";

        String expectedString = "+---------------------------------+\n" +
                                "| name      | score               |\n" +
                                "+---------------------------------+\n" +
                                "| \"Home\"    | 0.4015879109501838  |\n" +
                                "| \"Site A\"  | 0.1690742586266424  |\n" +
                                "| \"About\"   | 0.11305649263085797 |\n" +
                                "| \"Product\" | 0.11305649263085797 |\n" +
                                "| \"Links\"   | 0.11305649263085797 |\n" +
                                "| \"Site B\"  | 0.01907425862664241 |\n" +
                                "| \"Site C\"  | 0.01907425862664241 |\n" +
                                "| \"Site D\"  | 0.01907425862664241 |\n" +
                                "+---------------------------------+\n" +
                                "8 rows\n";

        assertEquals(expectedString, runQuery(q1, Result::resultAsString));

        String q2 =
            "MATCH (siteA:Page {name: 'Site A'})\n" +
            "CALL gds.pageRank.write('myGraph', {\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85,\n" +
            "  writeProperty: 'pagerank',\n" +
            "  sourceNodes: [siteA]\n" +
            "})\n" +
            "YIELD nodePropertiesWritten, ranIterations\n" +
            "RETURN nodePropertiesWritten AS writtenProperties, ranIterations";

        expectedString = "+-----------------------------------+\n" +
                         "| writtenProperties | ranIterations |\n" +
                         "+-----------------------------------+\n" +
                         "| 8                 | 20            |\n" +
                         "+-----------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, runQuery(q2, Result::resultAsString));
    }

    @Test
    void statsMode() {
        String q2 =
            "CALL gds.pageRank.stats('myGraph')\n" +
            "YIELD ranIterations";
        String r2 = runQuery(q2, Result::resultAsString);

        String expectedString = "+---------------+\n" +
                                "| ranIterations |\n" +
                                "+---------------+\n" +
                                "| 20            |\n" +
                                "+---------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, r2);
    }

    @Test
    void estimateMode() {
        String q2 =
            "CALL gds.pageRank.write.estimate('myGraph', {\n" +
            "  writeProperty: 'pageRank',\n" +
            "  maxIterations: 20,\n" +
            "  dampingFactor: 0.85\n" +
            "})\n" +
            "YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory";
        String r2 = runQuery(q2, Result::resultAsString);

        String expectedString = "+----------------------------------------------------------------------+\n" +
                                "| nodeCount | relationshipCount | bytesMin | bytesMax | requiredMemory |\n" +
                                "+----------------------------------------------------------------------+\n" +
                                "| 8         | 14                | 1536     | 1536     | \"1536 Bytes\"   |\n" +
                                "+----------------------------------------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, r2);
    }

}
