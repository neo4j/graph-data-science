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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Graph:
 * <pre>
 *         /-     (c)    -\
 * (a) - (b)               (d)
 *        \- (e) - (f) - /
 * </pre>
 */
class YensKSharedPrefixMaxDepthProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
            "CREATE (a:Node {name:'a'})\n" +
            "CREATE (b:Node {name:'b'})\n" +
            "CREATE (c:Node {name:'c'})\n" +
            "CREATE (d:Node {name:'d'})\n" +
            "CREATE (e:Node {name:'e'})\n" +
            "CREATE (f:Node {name:'f'})\n" +
            "CREATE" +
            " (a)-[:TYPE {cost:1.0}]->(b),\n" +
            " (b)-[:TYPE {cost:1.0}]->(c),\n" +
            " (c)-[:TYPE {cost:1.0}]->(d),\n" +
            " (b)-[:TYPE {cost:1.0}]->(e),\n" +
            " (e)-[:TYPE {cost:1.0}]->(f),\n" +
            " (f)-[:TYPE {cost:1.0}]->(d)\n";
        runQuery(cypher);
        registerProcedures(KShortestPathsProc.class);
    }

    @Test
    void testMaxDepthForKShortestPaths() {
        String algoCall = GdsCypher.call()
            .withRelationshipProperty("cost")
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .streamMode()
            .addVariable("startNode", "from")
            .addVariable("endNode", "to")
            .addParameter("k", 2)
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("path", true)
            .addPlaceholder("maxDepth", "maxDepth")
            .yields("path");

        final String cypher = String.format(
            "MATCH (from:Node{name:$from}), (to:Node{name:$to}) %s RETURN path",
            algoCall
        );

        Map<String, Object> params = new HashMap<>();
        params.put("from", "d");
        params.put("to", "a");
        params.put("maxDepth", 5);
        long pathsCount = runQuery(cypher, params, result -> result.stream().map(row -> row.get("path")).count());

        assertEquals(1, pathsCount, "Number of paths to maxDepth=5 should be 1");

        // Other direction should work ok, right?
        params.put("from", "a");
        params.put("to", "d");

        long pathsOtherDirectionCount = runQuery(cypher, params, result -> result.stream().map(row -> row.get("path")).count());

        assertEquals(1, pathsOtherDirectionCount, "Number of paths to maxDepth=5 should be 1");

        params.put("maxDepth", 6);

        long pathsDepth6Count = runQuery(cypher, params, result -> result.stream().map(row -> row.get("path")).count());

        assertEquals(2, pathsDepth6Count, "Number of paths to maxDepth=6 should be 2");
    }
}
