/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NeighborhoodSimilarityDocTest extends ProcTestBase {

    private static final String DB_CYPHER =
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

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
        );
        db.execute(DB_CYPHER);
        registerProcedures(NeighborhoodSimilarityProc.class);
        registerFunctions(GetNodeFunc.class);
    }

    @AfterEach
    void shutdown() {
        db.shutdown();
    }

    @Test
    void shouldProduceStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY similarity DESCENDING, Person1, Person2";

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

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void shouldProduceWriteOutput() {
        String query = "CALL algo.beta.jaccard('', 'LIKES', {" +
                       "  direction: 'OUTGOING'," +
                       "  write: true" +
                       "})" +
                       "YIELD nodesCompared, relationships, write, writeProperty, writeRelationshipType;";

        String expectedString = "+-------------------------------------------------------------------------------+\n" +
                                "| nodesCompared | relationships | write | writeProperty | writeRelationshipType |\n" +
                                "+-------------------------------------------------------------------------------+\n" +
                                "| 4             | 10            | true  | \"score\"       | \"SIMILAR\"             |\n" +
                                "+-------------------------------------------------------------------------------+\n" +
                                "1 row\n";

        assertEquals(expectedString, db.execute(query).resultAsString());

    }

    @Test
    void shouldProduceTopStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        topK: 1," +
                       "        topN: 3" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY similarity DESC, Person1, Person2";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "+----------------------------------------+\n" +
                                "3 rows\n";

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void shouldProduceTopKStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        topK: 1" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY Person1";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0                |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0                |\n" +
                                "+----------------------------------------+\n" +
                                "4 rows\n";

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void shouldProduceBottomKStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        bottomK: 1" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY Person1";

        String expectedString = "+----------------------------------------+\n" +
                                "| Person1 | Person2 | similarity         |\n" +
                                "+----------------------------------------+\n" +
                                "| \"Alice\" | \"Carol\" | 0.3333333333333333 |\n" +
                                "| \"Bob\"   | \"Alice\" | 0.6666666666666666 |\n" +
                                "| \"Carol\" | \"Alice\" | 0.3333333333333333 |\n" +
                                "| \"Dave\"  | \"Carol\" | 0.3333333333333333 |\n" +
                                "+----------------------------------------+\n" +
                                "4 rows\n";


        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void shouldProduceDegreeCutoffStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        degreeCutoff: 3" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY Person1";

        String expectedString = "+--------------------------------+\n" +
                                "| Person1 | Person2 | similarity |\n" +
                                "+--------------------------------+\n" +
                                "| \"Alice\" | \"Dave\"  | 1.0        |\n" +
                                "| \"Dave\"  | \"Alice\" | 1.0        |\n" +
                                "+--------------------------------+\n" +
                                "2 rows\n";

        assertEquals(expectedString, db.execute(query).resultAsString());
    }

    @Test
    void shouldProduceSimilarityCutoffStreamOutput() {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        direction: 'OUTGOING'," +
                       "        similarityCutoff: 0.5" +
                       "    }" +
                       ") YIELD node1, node2, similarity " +
                       "RETURN algo.asNode(node1).name AS Person1, algo.asNode(node2).name AS Person2, similarity " +
                       "ORDER BY Person1, similarity DESCENDING";

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

        assertEquals(expectedString, db.execute(query).resultAsString());
    }
}
