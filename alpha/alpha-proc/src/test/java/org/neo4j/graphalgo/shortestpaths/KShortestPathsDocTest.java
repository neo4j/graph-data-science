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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KShortestPathsDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();
    public static final String DB_CYPHER =
        "CREATE " +
        " (a:Loc {name:'A'})," +
        " (b:Loc {name:'B'})," +
        " (c:Loc {name:'C'})," +
        " (d:Loc {name:'D'})," +
        " (e:Loc {name:'E'})," +
        " (f:Loc {name:'F'})," +
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
        registerProcedures(KShortestPathsProc.class);
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = "MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})" +
                       " CALL gds.alpha.kShortestPaths.stream({ " +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'" +
                       "    }" +
                       "  }," +
                       "  startNode: start, " +
                       "  endNode: end, " +
                       "  k: 3," +
                       "  relationshipWeightProperty: 'cost' " +
                       "})" +
                       " YIELD index, nodeIds, costs " +
                       " RETURN [node in gds.util.asNodes(nodeIds) | node.name] AS places, " +
                       "       costs, " +
                       "       reduce(acc = 0.0, cost in costs | acc + cost) AS totalCost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------------------------------------------+" + NL + 
                          "| places                | costs                 | totalCost |" + NL + 
                          "+-----------------------------------------------------------+" + NL + 
                          "| [\"A\",\"B\",\"D\",\"E\",\"F\"] | [50.0,40.0,30.0,40.0] | 160.0     |" + NL + 
                          "| [\"A\",\"C\",\"D\",\"E\",\"F\"] | [50.0,40.0,30.0,40.0] | 160.0     |" + NL + 
                          "| [\"A\",\"B\",\"D\",\"F\"]     | [50.0,40.0,80.0]      | 170.0     |" + NL + 
                          "+-----------------------------------------------------------+" + NL + 
                          "3 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldStreamWithPath() {
        String query = "MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})" +
                       " CALL gds.alpha.kShortestPaths.stream({ " +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'" +
                       "    }" +
                       "  }," +
                       "  startNode: start, " +
                       "  endNode: end, " +
                       "  k: 3," +
                       "  relationshipWeightProperty: 'cost'," +
                       "  path: true " +
                       "})" +
                       " YIELD path " +
                       " RETURN path" +
                       " LIMIT 1";

        String actual = runQuery(query, Result::resultAsString);

        // Couldn't assert on the actual `path` because it wasn't the same between the test runs.
        assertFalse(actual.isEmpty());
    }

    @Test
    void shouldWrite() {
        String query = "MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})" +
                       " CALL gds.alpha.kShortestPaths.write({ " +
                       "  nodeProjection: 'Loc'," +
                       "  relationshipProjection: {" +
                       "    ROAD: {" +
                       "      type: 'ROAD'," +
                       "      properties: 'cost'" +
                       "    }" +
                       "  }," +
                       "  startNode: start, " +
                       "  endNode: end, " +
                       "  k: 3," +
                       "  relationshipWeightProperty: 'cost'" +
                       "})" +
                       " YIELD resultCount " +
                       " RETURN resultCount";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-------------+" + NL + 
                          "| resultCount |" + NL + 
                          "+-------------+" + NL + 
                          "| 3           |" + NL + 
                          "+-------------+" + NL + 
                          "1 row" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldWriteWithCypherProjection() {
        String query = "MATCH (start:Loc{name:'A'}), (end:Loc{name:'F'})" +
                       " CALL gds.alpha.kShortestPaths.write({ " +
                       "  nodeQuery:'MATCH(n:Loc) WHERE not n.name = \"C\" RETURN id(n) as id'," +
                       "  relationshipQuery:'MATCH (n:Loc)-[r:ROAD]->(m:Loc) RETURN id(n) as source, id(m) as target, r.cost as cost'," +
                       "  startNode: start, " +
                       "  endNode: end, " +
                       "  k: 3," +
                       "  relationshipWeightProperty: 'cost'," +
                       "  writePropertyPrefix:'cypher_'" +
                       "})" +
                       " YIELD resultCount " +
                       " RETURN resultCount";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-------------+" + NL +
                          "| resultCount |" + NL +
                          "+-------------+" + NL +
                          "| 3           |" + NL +
                          "+-------------+" + NL +
                          "1 row" + NL;

        assertEquals(expected, actual);
    }
}
