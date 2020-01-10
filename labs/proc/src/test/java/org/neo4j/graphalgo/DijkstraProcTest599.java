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

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.shortestpath.DijkstraProc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DijkstraProcTest599 extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (v1:Node {VID: 1})" +
        ", (v2:Node {VID: 2})" +
        ", (v3:Node {VID: 3})" +
        ", (v4:Node {VID: 4})" +
        ", (v5:Node {VID: 5})" +
        ", (v6:Node {VID: 6})" +
        ", (v7:Node {VID: 7})" +
        ", (v1)-[:EDGE {WEIGHT: 0.5}]->(v2)" +
        ", (v1)-[:EDGE {WEIGHT: 5.0}]->(v3)" +
        ", (v2)-[:EDGE {WEIGHT: 0.5}]->(v5)" +
        ", (v3)-[:EDGE {WEIGHT: 2.0}]->(v4)" +
        ", (v5)-[:EDGE {WEIGHT: 0.5}]->(v6)" +
        ", (v6)-[:EDGE {WEIGHT: 0.5}]->(v3)" +
        ", (v6)-[:EDGE {WEIGHT: 23.0}]->(v7)" +
        ", (v1)-[:EDGE {WEIGHT: 5.0}]->(v4)" +
        "";

    @BeforeEach
    void setupGraphDb() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(DijkstraProc.class);
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    /**
     * @see <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/599">Issue #599</a>
     */
    @Test
    void test599() {
        String totalCostCommand =
            "MATCH (startNode {VID: 1}), (endNode {VID: 4})\n" +
            "CALL gds.alpha.shortestPath.write({startNode: startNode, endNode: endNode, weightProperty: 'WEIGHT', relationshipProperties: 'WEIGHT'})\n" +
            "YIELD nodeCount, totalCost, loadMillis, evalMillis, writeMillis\n" +
            "RETURN totalCost\n";

        double totalCost = runQuery(totalCostCommand, result -> result
            .<Double>columnAs("totalCost")
            .stream()
            .findFirst()
            .orElse(Double.NaN)
        );

        assertEquals(4.0, totalCost, 1e-4);

        String pathCommand =
            "MATCH (startNode {VID: 1}), (endNode {VID: 4})\n" +
            "CALL gds.alpha.shortestPath.stream({startNode: startNode, endNode: endNode, weightProperty: 'WEIGHT', relationshipProperties: 'WEIGHT'})\n" +
            "YIELD nodeId, cost\n" +
            "MATCH (n1) WHERE id(n1) = nodeId\n" +
            "RETURN n1.VID AS id, cost AS weight\n";

        List<Matcher<Number>> expectedList = Arrays.asList(
            is(1L), is(0.0),
            is(2L), is(0.5),
            is(5L), is(1.0),
            is(6L), is(1.5),
            is(3L), is(2.0),
            is(4L), is(4.0)
        );
        Iterator<Matcher<Number>> expected = expectedList.iterator();

        runQueryWithResultConsumer(pathCommand, pathResult -> {
            pathResult.forEachRemaining(res -> {
                assertThat((Number) res.get("id"), expected.next());
                assertThat((Number) res.get("weight"), expected.next());
            });
        });
    }
}
