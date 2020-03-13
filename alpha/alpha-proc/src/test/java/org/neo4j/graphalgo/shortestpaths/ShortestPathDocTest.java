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
import org.neo4j.graphalgo.functions.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShortestPathDocTest extends BaseProcTest {
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
        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(DijkstraProc.class);
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = "MATCH (start:Loc {name: 'A'}), (end:Loc {name: 'F'})" +
                       " CALL gds.alpha.shortestPath.stream({" +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "        type: 'ROAD'," +
                       "        properties: 'cost'," +
                       "        projection: 'UNDIRECTED'" +
                       "      }" +
                       "    }," +
                       "    startNode: start," +
                       "    endNode: end," +
                       "    relationshipWeightProperty: 'cost'" +
                       "})" +
                       " YIELD nodeId, cost" +
                       " RETURN gds.util.asNode(nodeId).name AS name, cost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+--------------+" + NL +
                          "| name | cost  |" + NL +
                          "+--------------+" + NL +
                          "| \"A\"  | 0.0   |" + NL +
                          "| \"B\"  | 50.0  |" + NL +
                          "| \"D\"  | 90.0  |" + NL +
                          "| \"E\"  | 120.0 |" + NL +
                          "| \"F\"  | 160.0 |" + NL +
                          "+--------------+" + NL +
                          "5 rows" + NL;

        String expectedViaC = "+--------------+" + NL +
                          "| name | cost  |" + NL +
                          "+--------------+" + NL +
                          "| \"A\"  | 0.0   |" + NL +
                          "| \"C\"  | 50.0  |" + NL +
                          "| \"D\"  | 90.0  |" + NL +
                          "| \"E\"  | 120.0 |" + NL +
                          "| \"F\"  | 160.0 |" + NL +
                          "+--------------+" + NL +
                          "5 rows" + NL;

        assertTrue(Arrays.asList(expected, expectedViaC).contains(actual));
    }
    
    @Test
    void shouldWrite() {
        String query = "MATCH (start:Loc {name: 'A'}), (end:Loc {name: 'F'})" +
                       " CALL gds.alpha.shortestPath.write({" +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'," +
                       "      projection: 'UNDIRECTED'" +
                       "    }" +
                       "  }," +
                       "  startNode: start," +
                       "  endNode: end," +
                       "  relationshipWeightProperty: 'cost'," +
                       "  writeProperty: 'sssp'" +
                       "})" +
                       " YIELD nodeCount, totalCost" +
                       " RETURN nodeCount,totalCost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------+" + NL +
                          "| nodeCount | totalCost |" + NL +
                          "+-----------------------+" + NL +
                          "| 5         | 160.0     |" + NL +
                          "+-----------------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteCypher() {
        String query = "MATCH (start:Loc {name: 'A'}), (end:Loc {name: 'F'})" +
                       " CALL gds.alpha.shortestPath.write({" +
                       "  nodeQuery:'MATCH(n:Loc) WHERE NOT n.name = \"c\" RETURN id(n) AS id'," +
                       "  relationshipQuery:'MATCH(n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) AS source, id(m) AS target, r.cost AS weight'," +
                       "  startNode: start," +
                       "  endNode: end," +
                       "  relationshipWeightProperty: 'weight'," +
                       "  writeProperty: 'sssp'" +
                       "})" +
                       " YIELD nodeCount, totalCost" +
                       " RETURN nodeCount,totalCost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------+" + NL +
                          "| nodeCount | totalCost |" + NL +
                          "+-----------------------+" + NL +
                          "| 5         | 160.0     |" + NL +
                          "+-----------------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }
}
