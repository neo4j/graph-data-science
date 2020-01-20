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
package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BetweennessCentralityDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();
    private static final String DB_CYPHER =
        "CREATE" +
        "  (alice:User {name: 'Alice'})" +
        ", (bridget:User {name: 'Bridget'})" +
        ", (charles:User {name: 'Charles'})" +
        ", (doug:User {name: 'Doug'})" +
        ", (mark:User {name: 'Mark'})" +
        ", (michael:User {name: 'Michael'})" +
        ", (alice)-[:MANAGE]->(bridget)" +
        ", (alice)-[:MANAGE]->(charles)" +
        ", (alice)-[:MANAGE]->(doug)" +
        ", (mark)-[:MANAGE]->(alice)" +
        ", (charles)-[:MANAGE]->(michael)";


    @BeforeEach
    void setUp() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(BetweennessCentralityProc.class, SampledBetweennessCentralityProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldStream() {
        String query = " CALL gds.alpha.betweenness.stream({" +
                       "   nodeProjection: 'User'," +
                       "   relationshipProjection: 'MANAGE'," +
                       "   direction: 'OUTGOING'" +
                       " })" +
                       " YIELD nodeId, centrality" +
                       " RETURN gds.util.asNode(nodeId).name AS user, centrality" +
                       " ORDER BY centrality DESC";

        String expected = "+------------------------+" + NL +
                          "| user      | centrality |" + NL +
                          "+------------------------+" + NL +
                          "| \"Alice\"   | 4.0        |" + NL +
                          "| \"Charles\" | 2.0        |" + NL +
                          "| \"Bridget\" | 0.0        |" + NL +
                          "| \"Doug\"    | 0.0        |" + NL +
                          "| \"Mark\"    | 0.0        |" + NL +
                          "| \"Michael\" | 0.0        |" + NL +
                          "+------------------------+" + NL +
                          "6 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String query = " CALL gds.alpha.betweenness.write({" +
                       "   nodeProjection: 'User'," +
                       "   relationshipProjection: 'MANAGE'," +
                       "   direction: 'OUTGOING'," +
                       "   writeProperty: 'centrality'" +
                       " })" +
                       " YIELD nodes, minCentrality, maxCentrality, sumCentrality";

        String expected = "+-------------------------------------------------------+" + NL +
                          "| nodes | minCentrality | maxCentrality | sumCentrality |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "| 6     | 0.0           | 4.0           | 6.0           |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteWithCypherProjection() {
        String query = " CALL gds.alpha.betweenness.write({" +
                       "   nodeQuery: 'MATCH (p:User) RETURN id(p) as id'," +
                       "   relationshipQuery: 'MATCH (p1:User)-[:MANAGE]->(p2:User) RETURN id(p1) as source, id(p2) as target'" +
                       " }) YIELD nodes, minCentrality, maxCentrality, sumCentrality";

        String expected = "+-------------------------------------------------------+" + NL +
                          "| nodes | minCentrality | maxCentrality | sumCentrality |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "| 6     | 0.0           | 4.0           | 6.0           |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldStreamSampled() {
        String query = " CALL gds.alpha.betweenness.sampled.stream({" +
                       "   nodeProjection: 'User'," +
                       "   relationshipProjection: 'MANAGE'," +
                       "   strategy: 'random'," +
                       "   probability: 1.0," +
                       "   maxDepth: 1," +
                       "   direction: 'OUTGOING'" +
                       " }) YIELD nodeId, centrality" +
                       " RETURN gds.util.asNode(nodeId).name AS user, centrality" +
                       " ORDER BY centrality DESC";

        String expected = "+------------------------+" + NL +
                          "| user      | centrality |" + NL +
                          "+------------------------+" + NL +
                          "| \"Alice\"   | 3.0        |" + NL +
                          "| \"Charles\" | 1.0        |" + NL +
                          "| \"Bridget\" | 0.0        |" + NL +
                          "| \"Doug\"    | 0.0        |" + NL +
                          "| \"Mark\"    | 0.0        |" + NL +
                          "| \"Michael\" | 0.0        |" + NL +
                          "+------------------------+" + NL +
                          "6 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteSampled() {
        String query = " CALL gds.alpha.betweenness.sampled.write({" +
                       "   nodeProjection: 'User'," +
                       "   relationshipProjection: 'MANAGE'," +
                       "   strategy: 'random'," +
                       "   probability: 1.0," +
                       "   writeProperty: 'centrality'," +
                       "   maxDepth: 1," +
                       "   direction: 'OUTGOING'" +
                       " })" +
                       " YIELD nodes, minCentrality, maxCentrality, sumCentrality";

        String expected = "+-------------------------------------------------------+" + NL +
                          "| nodes | minCentrality | maxCentrality | sumCentrality |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "| 6     | 0.0           | 3.0           | 4.0           |" + NL +
                          "+-------------------------------------------------------+" + NL +
                          "1 row" + NL;

        String actual = runQuery(query, Result::resultAsString);

        assertEquals(expected, actual);
    }

}
