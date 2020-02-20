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

package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.shortestpaths.AllShortestPathsProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AllShortestPathsDocTest extends BaseProcTest {
    private static final String NL = System.lineSeparator();
    public static final String DB_CYPHER = "CREATE " +
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
        registerProcedures(AllShortestPathsProc.class);
        registerFunctions(IsFiniteFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        String query = " CALL gds.alpha.allShortestPaths.stream({" +
                       "   nodeProjection: 'Loc'," +
                       "   relationshipProjection: {" +
                       "     ROAD: {" +
                       "       type: 'ROAD'," +
                       "       properties: 'cost'," +
                       "       defaultValue: 1.0" +
                       "     }" +
                       "   }," +
                       "   relationshipWeightProperty: 'cost'" +
                       " })" +
                       " YIELD sourceNodeId, targetNodeId, distance" +
                       " WITH sourceNodeId, targetNodeId, distance" +
                       " WHERE gds.util.isFinite(distance) = true" +

                       " MATCH (source:Loc) WHERE id(source) = sourceNodeId" +
                       " MATCH (target:Loc) WHERE id(target) = targetNodeId" +
                       " WITH source, target, distance WHERE source <> target" +

                       " RETURN source.name AS source, target.name AS target, distance" +
                       " ORDER BY distance DESC, source ASC, target ASC" +
                       " LIMIT 10";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+----------------------------+" + NL +
                          "| source | target | distance |" + NL +
                          "+----------------------------+" + NL +
                          "| \"A\"    | \"F\"    | 160.0    |" + NL +
                          "| \"A\"    | \"E\"    | 120.0    |" + NL +
                          "| \"B\"    | \"F\"    | 110.0    |" + NL +
                          "| \"C\"    | \"F\"    | 110.0    |" + NL +
                          "| \"A\"    | \"D\"    | 90.0     |" + NL +
                          "| \"B\"    | \"E\"    | 70.0     |" + NL +
                          "| \"C\"    | \"E\"    | 70.0     |" + NL +
                          "| \"D\"    | \"F\"    | 70.0     |" + NL +
                          "| \"A\"    | \"B\"    | 50.0     |" + NL +
                          "| \"A\"    | \"C\"    | 50.0     |" + NL +
                          "+----------------------------+" + NL +
                          "10 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldStreamWithCypherProjection() {
        String query = " CALL gds.alpha.allShortestPaths.stream({" +
                       "   nodeQuery: 'MATCH (n:Loc) RETURN id(n) AS id'," +
                       "   relationshipQuery: 'MATCH (n:Loc)-[r:ROAD]-(p:Loc) RETURN id(n) AS source, id(p) AS target, r.cost AS cost'," +
                       "   relationshipWeightProperty: 'cost'" +
                       " })" +
                       " YIELD sourceNodeId, targetNodeId, distance" +
                       " WITH sourceNodeId, targetNodeId, distance" +
                       " WHERE gds.util.isFinite(distance) = true" +

                       " MATCH (source:Loc) WHERE id(source) = sourceNodeId" +
                       " MATCH (target:Loc) WHERE id(target) = targetNodeId" +
                       " WITH source, target, distance WHERE source <> target" +

                       " RETURN source.name AS source, target.name AS target, distance" +
                       " ORDER BY distance DESC, source ASC, target ASC" +
                       " LIMIT 10";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+----------------------------+" + NL +
                          "| source | target | distance |" + NL +
                          "+----------------------------+" + NL +
                          "| \"A\"    | \"F\"    | 160.0    |" + NL +
                          "| \"F\"    | \"A\"    | 160.0    |" + NL +
                          "| \"A\"    | \"E\"    | 120.0    |" + NL +
                          "| \"E\"    | \"A\"    | 120.0    |" + NL +
                          "| \"B\"    | \"F\"    | 110.0    |" + NL +
                          "| \"C\"    | \"F\"    | 110.0    |" + NL +
                          "| \"F\"    | \"B\"    | 110.0    |" + NL +
                          "| \"F\"    | \"C\"    | 110.0    |" + NL +
                          "| \"A\"    | \"D\"    | 90.0     |" + NL +
                          "| \"D\"    | \"A\"    | 90.0     |" + NL +
                          "+----------------------------+" + NL +
                          "10 rows" + NL;

        assertEquals(expected, actual);
    }
}
