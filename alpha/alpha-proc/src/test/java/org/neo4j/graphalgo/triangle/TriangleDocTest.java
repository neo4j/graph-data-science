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
package org.neo4j.graphalgo.triangle;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.functions.GetNodeFunc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TriangleDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();
    private static final String DB_CYPHER =
        "CREATE" +
        "  (alice:Person {name: 'Alice'})" +
        ", (michael:Person {name: 'Michael'})" +
        ", (karin:Person {name: 'Karin'})" +
        ", (chris:Person {name: 'Chris'})" +
        ", (will:Person {name: 'Will'})" +
        ", (mark:Person {name: 'Mark'})" +
        ", (michael)-[:KNOWS]->(karin)" +
        ", (michael)-[:KNOWS]->(chris)" +
        ", (will)-[:KNOWS]->(michael)" +
        ", (mark)-[:KNOWS]->(michael)" +
        ", (mark)-[:KNOWS]->(will)" +
        ", (alice)-[:KNOWS]->(michael)" +
        ", (will)-[:KNOWS]->(chris)" +
        ", (chris)-[:KNOWS]->(karin)";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(TriangleProc.class, TriangleCountStreamProc.class, TriangleCountWriteProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldStreamTriangles() {
        @Language("Cypher")
        String query = " CALL gds.alpha.triangle.stream({" +
                       "   nodeProjection: 'Person'," +
                       "   relationshipProjection: {" +
                       "     KNOWS: {" +
                       "       type: 'KNOWS'," +
                       "       orientation: 'UNDIRECTED'" +
                       "     }" +
                       "   }" +
                       " })" +
                       " YIELD nodeA, nodeB, nodeC" +
                       " RETURN gds.util.asNode(nodeA).name AS nodeA, gds.util.asNode(nodeB).name AS nodeB, gds.util.asNode(nodeC).name AS nodeC";

        String expected = "+-------------------------------+" + NL +
                          "| nodeA     | nodeB   | nodeC   |" + NL +
                          "+-------------------------------+" + NL +
                          "| \"Michael\" | \"Karin\" | \"Chris\" |" + NL +
                          "| \"Michael\" | \"Chris\" | \"Will\"  |" + NL +
                          "| \"Michael\" | \"Will\"  | \"Mark\"  |" + NL +
                          "+-------------------------------+" + NL +
                          "3 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteTriangleCount() {
        @Language("Cypher")
        String query = " CALL gds.triangleCount.write({" +
                       "   nodeProjection: 'Person'," +
                       "   relationshipProjection: {" +
                       "     KNOWS: {" +
                       "       type: 'KNOWS'," +
                       "       orientation: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   writeProperty: 'triangles'" +
                       " })" +
                       " YIELD nodeCount, triangleCount";

        String expected = "+---------------------------+" + NL +
                          "| nodeCount | triangleCount |" + NL +
                          "+---------------------------+" + NL +
                          "| 6         | 3             |" + NL +
                          "+---------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldStreamTriangleCount() {
        @Language("Cypher")
        String query = " CALL gds.triangleCount.stream({" +
                       "   nodeProjection: 'Person'," +
                       "   relationshipProjection: {" +
                       "     KNOWS: {" +
                       "       type: 'KNOWS'," +
                       "       orientation: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   concurrency: 4" +
                       " })" +
                       " YIELD nodeId, triangles" +
                       " RETURN gds.util.asNode(nodeId).name AS name, triangles" +
                       " ORDER BY triangles ASC";

        String expected = "+-----------------------+" + NL +
                          "| name      | triangles |" + NL +
                          "+-----------------------+" + NL +
                          "| \"Alice\"   | 0         |" + NL +
                          "| \"Karin\"   | 1         |" + NL +
                          "| \"Mark\"    | 1         |" + NL +
                          "| \"Chris\"   | 2         |" + NL +
                          "| \"Will\"    | 2         |" + NL +
                          "| \"Michael\" | 3         |" + NL +
                          "+-----------------------+" + NL +
                          "6 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    @Disabled("Requires the Yelp dataset, which we don't load for tests.")
    void shouldWriteTriangleCountForYelpDataset() {
        @Language("Cypher")
        String query = " CALL gds.triangleCount.write({" +
                       "   nodeProjection: 'Person'," +
                       "   relationshipProjection: {" +
                       "     FRIEND: {" +
                       "       type: 'FRIEND'," +
                       "       orientation: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   concurrency: 4," +
                       "   writeProperty: 'triangles'" +
                       " })" +
                       " YIELD nodeCount, triangleCount";

        String expected = "+---------------------------+" + NL +
                          "| nodeCount | triangleCount |" + NL +
                          "+---------------------------+" + NL +
                          "| ?         | ?             |" + NL +
                          "+---------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteWithCypherProjection() {
        String query = " CALL gds.triangleCount.write({" +
                       "   nodeQuery: 'MATCH (p:Person) RETURN id(p) AS id'," +
                       "   relationshipQuery: 'MATCH (p1:Person)-[:KNOWS]-(p2:Person) RETURN id(p1) AS source, id(p2) AS target'," +
                       "   writeProperty: 'triangle'" +
                       " }) YIELD nodeCount, triangleCount";

        String expected = "+---------------------------+" + NL +
                          "| nodeCount | triangleCount |" + NL +
                          "+---------------------------+" + NL +
                          "| 6         | 3             |" + NL +
                          "+---------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

}
