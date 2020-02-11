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
import org.neo4j.graphalgo.spanningtree.SpanningTreeProc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 *
 *         a                a
 *     1 /   \ 2          /  \
 *      /     \          /    \
 *     b --3-- c        b      c
 *     |       |   =>   |      |
 *     4       5        |      |
 *     |       |        |      |
 *     d --6-- e        d      e
 */
public class SpanningTreeProcTest extends BaseProcTest {

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @BeforeEach
    void setup() throws Exception {
        String cypher = "CREATE(a:Node {start: true}) " +
                        "CREATE(b:Node) " +
                        "CREATE(c:Node) " +
                        "CREATE(d:Node) " +
                        "CREATE(e:Node) " +
                        "CREATE(z:Node) " +
                        "CREATE (a)-[:TYPE {cost:1.0}]->(b) " +
                        "CREATE (a)-[:TYPE {cost:2.0}]->(c) " +
                        "CREATE (b)-[:TYPE {cost:3.0}]->(c) " +
                        "CREATE (b)-[:TYPE {cost:4.0}]->(d) " +
                        "CREATE (c)-[:TYPE {cost:5.0}]->(e) " +
                        "CREATE (d)-[:TYPE {cost:6.0}]->(e)";

        db = TestDatabaseCreator.createTestDatabase();
        runQuery(cypher);
        registerProcedures(SpanningTreeProc.class);
    }

    private long getStartNodeId() {
        return runQuery(
            "MATCH (n) WHERE n.start = true RETURN id(n) AS id",
            result -> result.<Long>columnAs("id").next()
        );
    }

    @Test
    void testMinimum() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Projection.UNDIRECTED)
            .withRelationshipProperty("cost", 1.0D)
            .algo("gds.alpha.spanningTree")
            .writeMode()
            .addParameter("startNodeId", getStartNodeId())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("weightWriteProperty", "cost")
            .yields("createMillis", "computeMillis", "writeMillis", "effectiveNodeCount");

        runQueryWithRowConsumer(
            query,
            res -> {
                assertNotEquals(-1L, res.getNumber("writeMillis").longValue());
                assertEquals(5, res.getNumber("effectiveNodeCount").intValue());
            }
        );

        final long relCount = runQuery(
            "MATCH (a)-[:MST]->(b) RETURN id(a) as a, id(b) as b",
            result -> result.stream().count()
        );

        assertEquals(relCount, 4);
    }

    @Test
    void testMaximum() {
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Projection.UNDIRECTED)
            .withRelationshipProperty("cost", 1.0D)
            .algo("gds.alpha.spanningTree.maximum")
            .writeMode()
            .addParameter("startNodeId", getStartNodeId())
            .addParameter("writeProperty", "MAX")
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("weightWriteProperty", "cost")
            .yields("createMillis", "computeMillis", "writeMillis", "effectiveNodeCount");

        runQueryWithRowConsumer(
            query,
            res -> {
                assertNotEquals(-1L, res.getNumber("writeMillis").longValue());
                assertEquals(5, res.getNumber("effectiveNodeCount").intValue());
            }
        );

        long relCount = runQuery(
            "MATCH (a)-[:MAX]->(b) RETURN id(a) as a, id(b) as b",
            result -> result.stream().count()
        );

        assertEquals(relCount, 4);
    }
}
