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
import org.neo4j.graphalgo.shortestpath.ShortestPathDeltaSteppingProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShortestPathDeltaSteppingDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();
    public static final String DB_CYPHER =
        "CREATE " +
        " (a:Loc {name: 'A'})," +
        " (b:Loc {name: 'B'})," +
        " (c:Loc {name: 'C'})," +
        " (d:Loc {name: 'D'})," +
        " (e:Loc {name: 'E'})," +
        " (f:Loc {name: 'F'})," +
        " (a)-[:ROAD {cost:50}]->(b)," +
        " (a)-[:ROAD {cost:50}]->(c)," +
        " (a)-[:ROAD {cost:100}]->(d)," +
        " (b)-[:ROAD {cost:40}]->(d)," +
        " (c)-[:ROAD {cost:40}]->(d)," +
        " (c)-[:ROAD {cost:80}]->(e)," +
        " (d)-[:ROAD {cost:30}]->(e)," +
        " (d)-[:ROAD {cost:80}]->(f)," +
        " (e)-[:ROAD {cost:40}]->(f);";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase(builder ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );

        registerProcedures(ShortestPathDeltaSteppingProc.class);
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = "MATCH (n:Loc {name: 'A'})" +
                       " CALL gds.alpha.shortestPath.deltaStepping.stream({" +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'" +
                       "    }" +
                       "  }," +
                       "  startNode: n," +
                       "  weightProperty: 'cost'," +
                       "  delta: 3.0" +
                       "})" +
                       " YIELD nodeId, distance" +
                       " RETURN gds.util.asNode(nodeId).name AS destination, distance";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+------------------------+" + NL +
                          "| destination | distance |" + NL +
                          "+------------------------+" + NL +
                          "| \"A\"         | 0.0      |" + NL +
                          "| \"B\"         | 50.0     |" + NL +
                          "| \"C\"         | 50.0     |" + NL +
                          "| \"D\"         | 90.0     |" + NL +
                          "| \"E\"         | 120.0    |" + NL +
                          "| \"F\"         | 160.0    |" + NL +
                          "+------------------------+" + NL +
                          "6 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String query = "MATCH (n:Loc {name: 'A'})" +
                       " CALL gds.alpha.shortestPath.deltaStepping.write({" +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'" +
                       "    }" +
                       "  }," +
                       "  startNode: n," +
                       "  weightProperty: 'cost'," +
                       "  delta: 3.0," +
                       "  writeProperty: 'sssp'" +
                       "})" +
                       " YIELD nodeCount" +
                       " RETURN nodeCount";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------+" + NL +
                          "| nodeCount |" + NL +
                          "+-----------+" + NL +
                          "| 6         |" + NL +
                          "+-----------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }
    
    @Test
    void shouldWriteCypher() {
        String query = "MATCH (start:Loc{name:'A'})" +
                       " CALL gds.alpha.shortestPath.deltaStepping.write({" +
                       "  nodeQuery:'MATCH(n:Loc) WHERE not n.name = \"c\" RETURN id(n) as id'," +
                       "  relationshipQuery:'MATCH(n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) as source, id(m) as target, r.cost as weight'," +
                       "  startNode: start," +
                       "  weightProperty: 'weight'," +
                       "  delta: 3.0," +
                       "  writeProperty: 'sssp'" +
                       "})" +
                       " YIELD nodeCount" +
                       " RETURN nodeCount";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------+" + NL +
                          "| nodeCount |" + NL +
                          "+-----------+" + NL +
                          "| 6         |" + NL +
                          "+-----------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }
}
