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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BalancedTriadsDocTest extends BaseProcTest {
    private static final String NL = System.lineSeparator();

    private static final String DB_CYPHER =
        "CREATE " +
        "   (a:Person {name:'Anna'})," +
        "   (b:Person {name:'Dolores'})," +
        "   (c:Person {name:'Matt'})," +
        "   (d:Person {name:'Larry'})," +
        "   (e:Person {name:'Stefan'})," +
        "   (f:Person {name:'Sophia'})," +
        "   (g:Person {name:'Robin'})," +
        "   (a)-[:TYPE {weight:1.0}]->(b)," +
        "   (a)-[:TYPE {weight:-1.0}]->(c)," +
        "   (a)-[:TYPE {weight:1.0}]->(d)," +
        "   (a)-[:TYPE {weight:-1.0}]->(e)," +
        "   (a)-[:TYPE {weight:1.0}]->(f)," +
        "   (a)-[:TYPE {weight:-1.0}]->(g)," +
        "   (b)-[:TYPE {weight:-1.0}]->(c)," +
        "   (c)-[:TYPE {weight:1.0}]->(d)," +
        "   (d)-[:TYPE {weight:-1.0}]->(e)," +
        "   (e)-[:TYPE {weight:1.0}]->(f)," +
        "   (f)-[:TYPE {weight:-1.0}]->(g)," +
        "   (g)-[:TYPE {weight:1.0}]->(b);";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );

        registerProcedures(BalancedTriadsProc.class);
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = "CALL gds.alpha.balancedTriads.stream({" +
                       "  nodeProjection: 'Person'," +
                       "  relationshipProjection: {" +
                       "    TYPE: {" +
                       "      type: 'TYPE'," +
                       "      properties: 'weight'," +
                       "      projection: 'UNDIRECTED'" +
                       "    }" +
                       "  }," +
                       "  relationshipWeightProperty: 'weight'" +
                       "})" +
                       " YIELD nodeId, balanced, unbalanced" +
                       " RETURN gds.util.asNode(nodeId).name AS person,balanced,unbalanced" +
                       " ORDER BY balanced + unbalanced DESC" +
                       " LIMIT 10";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------------------+" + NL +
                          "| person    | balanced | unbalanced |" + NL +
                          "+-----------------------------------+" + NL +
                          "| \"Anna\"    | 3        | 3          |" + NL +
                          "| \"Matt\"    | 1        | 1          |" + NL +
                          "| \"Larry\"   | 1        | 1          |" + NL +
                          "| \"Stefan\"  | 1        | 1          |" + NL +
                          "| \"Sophia\"  | 1        | 1          |" + NL +
                          "| \"Robin\"   | 1        | 1          |" + NL +
                          "| \"Dolores\" | 1        | 1          |" + NL +
                          "+-----------------------------------+" + NL +
                          "7 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String query = "CALL gds.alpha.balancedTriads.write({" +
                       "  nodeProjection: 'Person'," +
                       "  relationshipProjection: {" +
                       "    TYPE: {" +
                       "      type: 'TYPE'," +
                       "      properties: 'weight'," +
                       "      projection: 'UNDIRECTED'" +
                       "    }" +
                       "  }," +
                       "  relationshipWeightProperty: 'weight'" +
                       "})" +
                       " YIELD balancedProperty, balancedTriadCount, unbalancedProperty, unbalancedTriadCount;";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------------------------------------------------------------------+" + NL +
                          "| balancedProperty | balancedTriadCount | unbalancedProperty | unbalancedTriadCount |" + NL +
                          "+-----------------------------------------------------------------------------------+" + NL +
                          "| \"balanced\"       | 3                  | \"unbalanced\"       | 3                    |" + NL +
                          "+-----------------------------------------------------------------------------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }
}
