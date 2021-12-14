/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.shortestpaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.shortestpath.ShortestPathDeltaSteppingStreamProc;
import org.neo4j.gds.shortestpath.ShortestPathDeltaSteppingWriteProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShortestPathDeltaSteppingDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();

    @Neo4jGraph
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
        " (e)-[:ROAD {cost:40}]->(f)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(ShortestPathDeltaSteppingWriteProc.class, ShortestPathDeltaSteppingStreamProc.class, GraphCreateProc.class);
        registerFunctions(AsNodeFunc.class);

        runQuery("CALL gds.graph.create(" +
                 "  'graph'," +
                 "  'Loc'," +
                 "  {" +
                 "    ROAD: {" +
                 "      type: 'ROAD'," +
                 "      properties: 'cost'" +
                 "    }" +
                 "  }" +
                 ")");
    }

    @Test
    void shouldStream() {
        String query = "MATCH (n:Loc {name: 'A'})" +
                       " CALL gds.alpha.shortestPath.deltaStepping.stream('graph', {" +
                       "  startNode: n," +
                       "  relationshipWeightProperty: 'cost'," +
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
                       " CALL gds.alpha.shortestPath.deltaStepping.write('graph', {" +
                       "  startNode: n," +
                       "  relationshipWeightProperty: 'cost'," +
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
