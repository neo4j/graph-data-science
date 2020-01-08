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
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PageRankDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws KernelException {
        String createGraph = "CREATE (home:Page {name:'Home'})" +
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

        db = TestDatabaseCreator.createTestDatabase(builder ->
                builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
        );
        db.execute(createGraph);
        registerProcedures(PageRankStreamProc.class, PageRankWriteProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    // Queries and results match pagerank.adoc unweighted example section; should read from there in a future
    // Doesn't have any assertions; those should be to verify results with contents in pagerank.adoc
    // This is left for a future task
    @Test
    void unweighted() {
        String q1 =
                "CALL gds.pageRank.stream({ " +
                "   nodeProjection: 'Page', " +
                "   relationshipProjection: 'LINKS', " +
                "   maxIterations: 20, " +
                "   dampingFactor: 0.85" +
                "}) " +
                "YIELD nodeId, score " +
                "RETURN algo.asNode(nodeId).name AS name, score " +
                "ORDER BY score DESC ";

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

        assertEquals(expectedString, db.execute(q1).resultAsString());

        String q2 =
            "CALL gds.pageRank.write({" +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: 'LINKS'," +
            "  maxIterations: 20, " +
            "  dampingFactor: 0.85, " +
            "  writeProperty: 'pagerank'" +
            "})" +
            "YIELD nodePropertiesWritten AS writtenProperties, ranIterations, dampingFactor, writeProperty";
        String r2 = db.execute(q2).resultAsString();

        expectedString = "+-------------------------------------------------------------------+\n" +
                         "| writtenProperties | ranIterations | dampingFactor | writeProperty |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "| 8                 | 20            | 0.85          | \"pagerank\"    |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, r2);
    }

    // Queries and results match pagerank.adoc weighted example section
    // used to test that the results are correct in the docs
    @Test
    void weighted() {
        String q1 =
            "CALL gds.pageRank.stream({ " +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: {" +
            "    LINKS: {" +
            "      properties: ['weight']" +
            "    }" +
            "  }," +
            "  maxIterations: 20, " +
            "  dampingFactor: 0.85," +
            "  weightProperty: 'weight'" +
            "}) " +
            "YIELD nodeId, score " +
            "RETURN algo.asNode(nodeId).name AS name, score " +
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

        assertEquals(expectedString, db.execute(q1).resultAsString());


        String q2 =
            "CALL gds.pageRank.write({ " +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: {" +
            "    LINKS: {" +
            "      properties: ['weight']" +
            "    }" +
            "  }," +
            "  maxIterations: 20, " +
            "  dampingFactor: 0.85, " +
            "  writeProperty: 'pagerank', " +
            "  weightProperty: 'weight'" +
            "})" +
            "YIELD nodePropertiesWritten AS writtenProperties, ranIterations, dampingFactor, writeProperty";

        expectedString = "+-------------------------------------------------------------------+\n" +
                         "| writtenProperties | ranIterations | dampingFactor | writeProperty |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "| 8                 | 20            | 0.85          | \"pagerank\"    |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }

    @Test
    void personalized() {
        String q1 =
            "MATCH (siteA:Page {name: 'Site A'})" +
            "CALL gds.pageRank.stream({ " +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: 'LINKS', " +
            "  maxIterations: 20, " +
            "  dampingFactor: 0.85, " +
            "  sourceNodes: [siteA]" +
            "}) " +
            "YIELD nodeId, score " +
            "RETURN algo.asNode(nodeId).name AS name, score " +
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

        assertEquals(expectedString, db.execute(q1).resultAsString());

        String q2 =
            "MATCH (siteA:Page {name: 'Site A'})" +
            "CALL gds.pageRank.write({ " +
            "   nodeProjection: 'Page', " +
            "   relationshipProjection: 'LINKS', " +
            "   maxIterations: 20, " +
            "   dampingFactor: 0.85, " +
            "   writeProperty: 'pagerank', " +
            "   sourceNodes: [siteA]})" +
            "YIELD nodePropertiesWritten, ranIterations, dampingFactor, writeProperty " +
            "RETURN nodePropertiesWritten AS writtenProperties, ranIterations, dampingFactor, writeProperty";

        expectedString = "+-------------------------------------------------------------------+\n" +
                         "| writtenProperties | ranIterations | dampingFactor | writeProperty |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "| 8                 | 20            | 0.85          | \"pagerank\"    |\n" +
                         "+-------------------------------------------------------------------+\n" +
                         "1 row\n";

        assertEquals(expectedString, db.execute(q2).resultAsString());
    }

    // Queries from the named graph and Cypher projection example in pagerank.adoc
    // used to test that the results are correct in the docs
    @Test
    void namedGraphAndCypherProjection() {
        String loadGraph = "CALL gds.graph.create('myGraph', ['Page'], ['LINKS'])";
        db.execute(loadGraph);

        String q1 =
            "CALL gds.pageRank.stream('myGraph')" +
            "YIELD nodeId, score " +
            "RETURN algo.asNode(nodeId).name AS name, score " +
            "ORDER BY score DESC ";

        String namedQueryResult = db.execute(q1).resultAsString();

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

        assertEquals(expectedString, namedQueryResult);

        String q2 =
            "CALL gds.pageRank.stream({" +
            "  nodeQuery: 'MATCH (p:Page) RETURN id(p) AS id'," +
            "  relationshipQuery: 'MATCH (p1:Page)-[:LINKS]->(p2:Page)" +
            "                      RETURN id(p1) AS source, id(p2) AS target'," +
            "   maxIterations:20," +
            "   dampingFactor:0.85" +
            "})" +
            "YIELD nodeId, score " +
            "RETURN algo.asNode(nodeId).name AS name, score " +
            "ORDER BY score DESC";

        assertEquals(namedQueryResult, db.execute(q2).resultAsString());
    }

    @Test
    void statsMode() {
        String q2 =
            "CALL gds.pageRank.stats({" +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: 'LINKS'," +
            "  maxIterations: 20, " +
            "  dampingFactor: 0.85, " +
            "  writeProperty: 'pagerank'" +
            "})" +
            "YIELD ranIterations, dampingFactor";
        String r2 = db.execute(q2).resultAsString();

        String expectedString = "+-------------------------------+\n" +
                                "| ranIterations | dampingFactor |\n" +
                                "+-------------------------------+\n" +
                                "| 20            | 0.85          |\n" +
                                "+-------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, r2);
    }

    @Test
    void estimateMode() {
        String q2 =
            "CALL gds.pageRank.write.estimate({" +
            "  nodeProjection: 'Page', " +
            "  relationshipProjection: 'LINKS'," +
            "  writeProperty: 'pagerank'," +
            "  concurrency: 1"+
            "})" +
            "YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory";
        String r2 = db.execute(q2).resultAsString();

        String expectedString = "+----------------------------------------------------------------------+\n" +
                                "| nodeCount | relationshipCount | bytesMin | bytesMax | requiredMemory |\n" +
                                "+----------------------------------------------------------------------+\n" +
                                "| 8         | 14                | 304064   | 304064   | \"296 KiB\"      |\n" +
                                "+----------------------------------------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, r2);
    }

}
