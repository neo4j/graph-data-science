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
package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


/**
 * Test for <a href="https://github.com/neo4j-contrib/neo4j-graph-algorithms/issues/282">Issue 282</a>
 *
 *    (B)--.    (F)
 *    /     \  /   \
 *  (A)-(C)-(E)   (H)
 *    \     /  \  /
 *    (D)--´   (G)
 */
@ExtendWith(MockitoExtension.class)
class BetweennessCentralityProcTest_282 extends BaseProcTest {

    private static final double[] EXPECTED = {
            0.0,
            1.33333,
            1.33333,
            1.33333,
            12.0,
            2.5,
            2.5,
            0
    };

    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node{id:'A'})" +
        ", (b:Node{id:'B'})" +
        ", (c:Node{id:'C'})" +
        ", (d:Node{id:'D'})" +
        ", (e:Node{id:'E'})" +
        ", (f:Node{id:'F'})" +
        ", (g:Node{id:'G'})" +
        ", (h:Node{id:'H'})" +
        ", (a)-[:EDGE]->(b)" +
        ", (a)-[:EDGE]->(c)" +
        ", (a)-[:EDGE]->(d)" +
        ", (b)-[:EDGE]->(e)" +
        ", (c)-[:EDGE]->(e)" +
        ", (d)-[:EDGE]->(e)" +
        ", (e)-[:EDGE]->(f)" +
        ", (e)-[:EDGE]->(g)" +
        ", (f)-[:EDGE]->(h)" +
        ", (g)-[:EDGE]->(h)";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(BetweennessCentralityProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void testBCWriteBack() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("EDGE")
            .algo("gds.alpha.betweenness")
            .writeMode()
            .addParameter("concurrency", 1)
            .yields("nodes", "minCentrality", "maxCentrality", "sumCentrality");

        runQuery(query);

        String checkQuery = "MATCH (n) WHERE exists(n.centrality) RETURN id(n) as id, n.centrality as c";
        double[] result = new double[EXPECTED.length];
        runQueryWithRowConsumer(checkQuery, row -> {
            int id = row.getNumber("id").intValue();
            double c = row.getNumber("c").doubleValue();
            result[id] = c;
        });

        assertArrayEquals(EXPECTED, result, 0.1);
    }

    @Test
    void testBCWriteBackParallel() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("EDGE")
            .algo("gds.alpha.betweenness")
            .writeMode()
            .addParameter("concurrency", 4)
            .yields("nodes", "minCentrality", "maxCentrality", "sumCentrality");

        runQuery(query);

        String checkQuery = "MATCH (n) WHERE exists(n.centrality) RETURN id(n) as id, n.centrality as c";
        double[] result = new double[EXPECTED.length];
        runQueryWithRowConsumer(checkQuery, row -> {
            int id = row.getNumber("id").intValue();
            double c = row.getNumber("c").doubleValue();
            result[id] = c;
        });

        assertArrayEquals(EXPECTED, result, 0.1);
    }
}
