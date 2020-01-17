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

public class ClosenessCentralityDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    public static final String DB_CYPHER =
        "CREATE " +
       "  (a:Node{name: \"A\"})" +
       " ,(b:Node{name: \"B\"})" +
       " ,(c:Node{name: \"C\"})" +
       " ,(d:Node{name: \"D\"})" +
       " ,(e:Node{name: \"E\"})" +
       " ,(a)-[:LINK]->(b)" +
       " ,(b)-[:LINK]->(a)" +
       " ,(b)-[:LINK]->(c)" +
       " ,(c)-[:LINK]->(b)" +
       " ,(c)-[:LINK]->(d)" +
       " ,(d)-[:LINK]->(c)" +
       " ,(d)-[:LINK]->(e)" +
       " ,(e)-[:LINK]->(d);";

    @BeforeEach
    void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.*")
        );

        registerProcedures(ClosenessCentralityProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void clearCommunities() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldStream() {
        String query =
            "CALL gds.alpha.closeness.stream({" +
            "  nodeProjection: 'Node'," +
            "  relationshipProjection: 'LINK'" +
            "}) YIELD nodeId, centrality" +
            " RETURN gds.util.asNode(nodeId).name AS user, centrality" +
            " ORDER BY centrality DESC";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+---------------------------+" + NL + 
                          "| user | centrality         |" + NL + 
                          "+---------------------------+" + NL + 
                          "| \"C\"  | 0.6666666666666666 |" + NL + 
                          "| \"B\"  | 0.5714285714285714 |" + NL + 
                          "| \"D\"  | 0.5714285714285714 |" + NL + 
                          "| \"A\"  | 0.4                |" + NL + 
                          "| \"E\"  | 0.4                |" + NL + 
                          "+---------------------------+" + NL + 
                          "5 rows" + NL;
        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String query =
            "CALL gds.alpha.closeness.write({" +
            "  nodeProjection: 'Node'," +
            "  relationshipProjection: 'LINK'," +
            "  writeProperty: 'centrality'" +
            "}) YIELD nodes, writeProperty";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------+" + NL +
                          "| nodes | writeProperty |" + NL +
                          "+-----------------------+" + NL +
                          "| 5     | \"centrality\"  |" + NL +
                          "+-----------------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteWithCypherProjection() {
        String query = "CALL gds.alpha.closeness.write({" + NL + 
                       "  nodeQuery: 'MATCH (p:Node) RETURN id(p) as id'," + NL + 
                       "  relationshipQuery: 'MATCH (p1:Node)-[:LINK]->(p2:Node) RETURN id(p1) as source, id(p2) as target'" + NL + 
                       "}) YIELD nodes, writeProperty";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------+" + NL +
                          "| nodes | writeProperty |" + NL +
                          "+-----------------------+" + NL +
                          "| 5     | \"centrality\"  |" + NL +
                          "+-----------------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }

}
