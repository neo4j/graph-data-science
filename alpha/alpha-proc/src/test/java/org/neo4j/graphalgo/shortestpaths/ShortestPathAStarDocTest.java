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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.functions.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShortestPathAStarDocTest extends BaseProcTest {

    private static final String NL = System.lineSeparator();
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Station {name: 'King\\'s Cross St. Pancras', latitude: 51.5308, longitude: -0.1238})" +
        ", (b:Station {name: 'Euston',                     latitude: 51.5282, longitude: -0.1337})" +
        ", (c:Station {name: 'Camden Town',                latitude: 51.5392, longitude: -0.1426})" +
        ", (d:Station {name: 'Mornington Crescent',        latitude: 51.5342, longitude: -0.1387})" +
        ", (e:Station {name: 'Kentish Town',               latitude: 51.5507, longitude: -0.1402})" +
        ", (a)-[:CONNECTION {time: 2}]->(b)" +
        ", (b)-[:CONNECTION {time: 3}]->(c)" +
        ", (b)-[:CONNECTION {time: 2}]->(d)" +
        ", (d)-[:CONNECTION {time: 2}]->(c)" +
        ", (c)-[:CONNECTION {time: 2}]->(e)";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(ShortestPathAStarProc.class);
        registerFunctions(GetNodeFunc.class);

        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void shouldStream() {
        @Language("Cypher")
        String query = " MATCH (start:Station {name: 'King\\'s Cross St. Pancras'}), (end:Station {name: 'Kentish Town'})" +
                       " CALL gds.alpha.shortestPath.astar.stream({" +
                       "   nodeProjection: '*'," +
                       "   relationshipProjection: {" +
                       "     CONNECTION: {" +
                       "       type: 'CONNECTION'," +
                       "       projection: 'UNDIRECTED'," +
                       "       properties: 'time'" +
                       "     }" +
                       "   }," +
                       "   startNode: start," +
                       "   endNode: end," +
                       "   relationshipWeightProperty: 'time'," +
                       "   propertyKeyLat: 'latitude'," +
                       "   propertyKeyLon: 'longitude'" +
                       "})" +
                       " YIELD nodeId, cost" +
                       " RETURN gds.util.asNode(nodeId).name AS station, cost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------------------+" + NL +
                          "| station                    | cost |" + NL +
                          "+-----------------------------------+" + NL +
                          "| \"King's Cross St. Pancras\" | 0.0  |" + NL +
                          "| \"Euston\"                   | 2.0  |" + NL +
                          "| \"Camden Town\"              | 5.0  |" + NL +
                          "| \"Kentish Town\"             | 7.0  |" + NL +
                          "+-----------------------------------+" + NL +
                          "4 rows" + NL;

        assertEquals(expected, actual);
    }

    @Test
    void shouldCypherStream() {
        @Language("Cypher")
        String query = " MATCH (start:Station {name: \"King's Cross St. Pancras\"}), (end:Station {name: \"Kentish Town\"})" +
                       " CALL gds.alpha.shortestPath.astar.stream({" +
                       "  nodeQuery: 'MATCH (p:Station) RETURN id(p) AS id'," +
                       "  relationshipQuery: 'MATCH (p1:Station)-[r:CONNECTION]->(p2:Station) RETURN id(p1) AS source, id(p2) AS target, r.time AS time'," +
                       "  startNode: start," +
                       "  endNode: end," +
                       "  relationshipWeightProperty: 'time'," +
                       "  propertyKeyLat: 'latitude'," +
                       "  propertyKeyLat: 'longitude'" +
                       " })" +
                       " YIELD nodeId, cost" +
                       " RETURN gds.util.asNode(nodeId).name AS station, cost";

        String actual = runQuery(query, Result::resultAsString);
        String expected = "+-----------------------------------+" + NL +
                          "| station                    | cost |" + NL +
                          "+-----------------------------------+" + NL +
                          "| \"King's Cross St. Pancras\" | 0.0  |" + NL +
                          "| \"Euston\"                   | 2.0  |" + NL +
                          "| \"Camden Town\"              | 5.0  |" + NL +
                          "| \"Kentish Town\"             | 7.0  |" + NL +
                          "+-----------------------------------+" + NL +
                          "4 rows" + NL;

        assertEquals(expected, actual);
    }

}
