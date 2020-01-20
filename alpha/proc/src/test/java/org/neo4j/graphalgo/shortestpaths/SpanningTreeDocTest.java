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

package org.neo4j.graphalgo.shortestpaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.spanningtree.KSpanningTreeProc;
import org.neo4j.graphalgo.spanningtree.SpanningTreeProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

// TODO: Update the tests after https://trello.com/c/EWgz8KfE is fixed
class SpanningTreeDocTest extends BaseProcTest {
    private static final String NL = System.lineSeparator();

    private static final String DB_CYPHER =
        "CREATE " +
        " (a:Place {id:\"A\"})," +
        " (b:Place {id:\"B\"})," +
        " (c:Place {id:\"C\"})," +
        " (d:Place {id:\"D\"})," +
        " (e:Place {id:\"E\"})," +
        " (f:Place {id:\"F\"})," +
        " (g:Place {id:\"G\"})," +
        " (d)-[:LINK {cost:4}]->(b)," +
        " (d)-[:LINK {cost:6}]->(e)," +
        " (b)-[:LINK {cost:1}]->(a)," +
        " (b)-[:LINK {cost:3}]->(c)," +
        " (a)-[:LINK {cost:2}]->(c)," +
        " (c)-[:LINK {cost:5}]->(e)," +
        " (f)-[:LINK {cost:1}]->(g);";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );
        registerProcedures(
            SpanningTreeProc.class,
            KSpanningTreeProc.class
        );
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldWriteMinimumWeightSpanningTree() {
        String spanningTreeQuery = "MATCH (n:Place {id:\"D\"})" +
                       " CALL gds.alpha.spanningTree.minimum.write({"+
                       "   nodeProjection: 'Place'," +
                       "   relationshipProjection: {" +
                       "     LINK: {" +
                       "       type: 'LINK'," +
                       "       properties: 'cost'," +
                       "       projection: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   startNodeId: id(n)," +
                       "   weightProperty: 'cost'," +
                       "   weightWriteProperty: 'writeCost'," +
                       "   writeProperty: 'MINST'" +
                       " })" +
                       " YIELD createMillis, computeMillis, writeMillis, effectiveNodeCount" +
                       " RETURN createMillis, computeMillis, writeMillis, effectiveNodeCount;";

        runQuery(spanningTreeQuery);

        // Can't assert on the written cost because of currently open bug: https://trello.com/c/EWgz8KfE
        String query = "MATCH path = (n:Place {id:\"D\"})-[:MINST*]-()" + NL +
                       "WITH relationships(path) AS rels" + NL +
                       "UNWIND rels AS rel" + NL +
                       "WITH DISTINCT rel AS rel" + NL +
                       "RETURN startNode(rel).id AS source, endNode(rel).id AS destination";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+----------------------+" + NL +
                          "| source | destination |" + NL +
                          "+----------------------+" + NL +
                          "| \"D\"    | \"B\"         |" + NL +
                          "| \"B\"    | \"A\"         |" + NL +
                          "| \"A\"    | \"C\"         |" + NL +
                          "| \"C\"    | \"E\"         |" + NL +
                          "+----------------------+" + NL +
                          "4 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteMaximumWeightSpanningTree() {
        String spanningTreeQuery = "MATCH (n:Place {id:\"D\"})" +
                       " CALL gds.alpha.spanningTree.maximum.write({"+
                       "   nodeProjection: 'Place'," +
                       "   relationshipProjection: {" +
                       "     LINK: {" +
                       "       type: 'LINK'," +
                       "       properties: 'cost'," +
                       "       projection: 'UNDIRECTED'" +
                       "     }" +
                       "   }," +
                       "   startNodeId: id(n)," +
                       "   weightProperty: 'cost'," +
                       "   weightWriteProperty: 'writeCost'," + // -> the weight of the `writeProperty` relationship
                       "   writeProperty: 'MAXST'" + // -> type of the new relationship
                       " })" +
                       " YIELD createMillis, computeMillis, writeMillis, effectiveNodeCount" +
                       " RETURN createMillis, computeMillis, writeMillis, effectiveNodeCount;";

        runQuery(spanningTreeQuery);

        // Can't assert on the written cost because of currently open bug: https://trello.com/c/EWgz8KfE
        String query = "MATCH path = (n:Place {id:\"D\"})-[:MAXST*]-()" + NL +
                       "WITH relationships(path) AS rels" + NL +
                       "UNWIND rels AS rel" + NL +
                       "WITH DISTINCT rel AS rel" + NL +
                       "RETURN startNode(rel).id AS source, endNode(rel).id AS destination";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+----------------------+" + NL +
                          "| source | destination |" + NL +
                          "+----------------------+" + NL +
                          "| \"D\"    | \"B\"         |" + NL +
                          "| \"D\"    | \"E\"         |" + NL +
                          "| \"E\"    | \"C\"         |" + NL +
                          "| \"C\"    | \"A\"         |" + NL +
                          "+----------------------+" + NL +
                          "4 rows" + NL;

        assertEquals(expected, actual);
    }
}
