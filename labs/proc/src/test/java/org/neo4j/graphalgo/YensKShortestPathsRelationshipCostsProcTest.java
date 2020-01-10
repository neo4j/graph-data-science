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
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.shortestpath.KShortestPathsProc;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Graph:
 * <p>
 * (0)
 * /  |  \
 * (4)--(5)--(1)
 * \  /  \ /
 * (3)---(2)
 */
class YensKShortestPathsRelationshipCostsProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE" +
                " (a)-[:TYPE {cost:3.0}]->(b),\n" +
                " (b)-[:TYPE {cost:2.0}]->(c)";

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
            .addVariable("startNode", "c")
            .addVariable("endNode", "a")
            .addParameter("k", 1)
            .addParameter("weightProperty", "cost")
            .yields("resultCount");

        String cypher = String.format(
            "MATCH (c:Node{name:'c'}), (a:Node{name:'a'}) %s RETURN resultCount",
            algoCall
        );

        runQueryWithRowConsumer(cypher, row -> assertEquals(1, row.getNumber("resultCount").intValue()));

        String shortestPathsQuery =
            "MATCH p=(:Node {name: $one})-[r:PATH_0*]->(:Node {name: $two}) " +
            "UNWIND relationships(p) AS pair " +
            "RETURN sum(pair.weight) AS distance";

        runQueryWithRowConsumer(
            shortestPathsQuery,
            MapUtil.map("one", "c", "two", "a"),
            row -> assertEquals(5.0, row.getNumber("distance").doubleValue(), 0.01)
        );
    }
}
