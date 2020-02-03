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
package org.neo4j.graphalgo.nodesim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeSimilarityDocTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );
        registerProcedures(NodeSimilarityStreamProc.class, NodeSimilarityWriteProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);

        String dbQuery =
            "CREATE (alice:Person {name: 'Alice'})" +
            "CREATE (bob:Person {name: 'Bob'})" +
            "CREATE (carol:Person {name: 'Carol'})" +
            "CREATE (dave:Person {name: 'Dave'})" +
            "CREATE (eve:Person {name: 'Eve'})" +
            "CREATE (guitar:Instrument {name: 'Guitar'})" +
            "CREATE (synth:Instrument {name: 'Synthesizer'})" +
            "CREATE (bongos:Instrument {name: 'Bongos'})" +
            "CREATE (trumpet:Instrument {name: 'Trumpet'})" +

            "CREATE (alice)-[:LIKES]->(guitar)" +
            "CREATE (alice)-[:LIKES]->(synth)" +
            "CREATE (alice)-[:LIKES]->(bongos)" +
            "CREATE (bob)-[:LIKES]->(guitar)" +
            "CREATE (bob)-[:LIKES]->(synth)" +
            "CREATE (carol)-[:LIKES]->(bongos)" +
            "CREATE (dave)-[:LIKES]->(guitar)" +
            "CREATE (dave)-[:LIKES]->(synth)" +
            "CREATE (dave)-[:LIKES]->(bongos);";

        String createGraphQuery = "CALL gds.graph.create('myGraph', 'Person | Instrument', 'LIKES')";

        runQuery(dbQuery);
        runQuery(createGraphQuery);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void shouldProduceStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph')\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY similarity DESCENDING, Person1, Person2\n";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "| \"Alice\" | \"Bob\"   | 0.6666666666666666 |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Bob\"   | \"Dave\"  | 0.6666666666666666 |\n" +
                                "| \"Dave\"  | \"Bob\"   | 0.6666666666666666 |\n" +
                                "| \"Alice\" | \"Carol\" | 0.3333333333333333 |\n" +
                                "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                "| \"Carol\" | \"Dave\"  | 0.3333333333333333 |\n" +
                                "| \"Dave\"  | \"Carol\" | 0.3333333333333333 |\n" +
                                "+----------------------------------------+\n" +
                                "10 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceWriteOutput() {
        String query = "CALL gds.nodeSimilarity.write('myGraph', {\n" +
                       "    writeRelationshipType: 'SIMILAR',\n" +
                       "    writeProperty: 'score'\n" +
                       "})\n" +
                       "YIELD nodesCompared, relationshipsWritten;";

        String expectedString = "+--------------------------------------+\n" +
                                "| nodesCompared | relationshipsWritten |\n" +
                                "+--------------------------------------+\n" +
                                "| 4             | 10                   |\n" +
                                "+--------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceTopStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph', {\n" +
                       "  topK: 1,\n" +
                       "  topN: 3\n" +
                       "})\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY similarity DESC, Person1, Person2\n";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "+----------------------------------------+\n" +
                                "3 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceTopKStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph', { topK: 1 })\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY Person1\n";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "+----------------------------------------+\n" +
                                "4 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceBottomKStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph', { bottomK: 1 })\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY Person1\n";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Carol\" | 0.3333333333333333 |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                "| \"Dave\"  | \"Carol\" | 0.3333333333333333 |\n" +
                                "+----------------------------------------+\n" +
                                "4 rows\n";


        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceDegreeCutoffStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph', { degreeCutoff: 3 })\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY Person1\n";

        String expectedString = "+--------------------------------+\n" +
                                "| Person1 | Person2 | similarity |\n" +
                                "+--------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0        |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0        |\n" +
                                "+--------------------------------+\n" +
                                "2 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }

    @Test
    void shouldProduceSimilarityCutoffStreamOutput() {
        String query = "CALL gds.nodeSimilarity.stream('myGraph', { similarityCutoff: 0.5 })\n" +
                       "YIELD node1, node2, similarity\n" +
                       "RETURN gds.util.asNode(node1).name AS Person1, gds.util.asNode(node2).name AS Person2, similarity\n" +
                       "ORDER BY Person1\n";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Alice\" | \"Bob\"   | 0.6666666666666666 |\n" +
                                "| \"Bob\"   | \"Dave\"  | 0.6666666666666666 |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "| \"Dave\"  | \"Bob\"   | 0.6666666666666666 |\n" +
                                "+----------------------------------------+\n" +
                                "6 rows\n";

        assertEquals(expectedString, runQuery(query, Result::resultAsString));
    }
}
