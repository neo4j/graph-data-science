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
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Graph:
 *
 *            (0)
 *          /  |  \
 *       (4)--(5)--(1)
 *         \  /  \ /
 *         (3)---(2)
 *
 */
class YensKShortestPathsProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        final String cypher =
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
                " (e)-[:TYPE {cost:1.0}]->(d),\n" +
                " (a)-[:TYPE {cost:1.0}]->(e),\n" +

                " (a)-[:TYPE {cost:5.0}]->(f),\n" +
                " (b)-[:TYPE {cost:4.0}]->(f),\n" +
                " (c)-[:TYPE {cost:1.0}]->(f),\n" +
                " (d)-[:TYPE {cost:1.0}]->(f),\n" +
                " (e)-[:TYPE {cost:4.0}]->(f)";

        runQuery(cypher);
        registerProcedures(KShortestPathsProc.class);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    @Test
    void test() {
        String algoCall = GdsCypher.call()
            .withRelationshipProperty("cost")
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .writeMode()
            .addVariable("startNode", "a")
            .addVariable("endNode", "f")
            .addParameter("k", 42)
            .addParameter("relationshipWeightProperty", "cost")
            .yields("resultCount");

        String cypher = String.format("MATCH (a:Node{name:'a'}), (f:Node{name:'f'}) %s RETURN resultCount", algoCall);

        // 9 possible paths without loop
        runQueryWithRowConsumer(cypher, row -> assertEquals(9, row.getNumber("resultCount").intValue()));

        /*
         * 10 rels from source graph already in db
         * + 29 rels from 9 paths
         */
        long actual = QueryRunner.runInTransaction(db, () -> db.getAllRelationships().stream().count());
        assertEquals(39, actual);

        Map<String, Double> combinations = new HashMap<>();
        combinations.put("PATH_0", 3.0);
        combinations.put("PATH_1", 3.0);
        combinations.put("PATH_2", 4.0);
        combinations.put("PATH_3", 4.0);
        combinations.put("PATH_4", 5.0);
        combinations.put("PATH_5", 5.0);
        combinations.put("PATH_6", 5.0);
        combinations.put("PATH_7", 8.0);
        combinations.put("PATH_8", 8.0);

        String shortestPathQueryTemplate =
            "MATCH p=(:Node {name: 'a'})-[r:%s*]->(:Node {name: 'f'}) " +
            "UNWIND relationships(p) AS pair " +
            "return sum(pair.weight) AS distance";
        for (String relationshipType : combinations.keySet()) {
            String shortestPathsQuery = String.format(
                shortestPathQueryTemplate,
                relationshipType
            );

            runQueryWithRowConsumer(
                shortestPathsQuery,
                row -> assertEquals(combinations.get(relationshipType), row.getNumber("distance").doubleValue(), 0.01)
            );
        }
    }
}
