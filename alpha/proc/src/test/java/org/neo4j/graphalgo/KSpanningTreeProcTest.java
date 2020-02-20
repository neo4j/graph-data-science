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
import org.neo4j.graphalgo.spanningtree.KSpanningTreeProc;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KSpanningTreeProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +

                "CREATE" +
                " (a)-[:TYPE {w:3.0}]->(b),\n" +
                " (a)-[:TYPE {w:2.0}]->(c),\n" +
                " (a)-[:TYPE {w:1.0}]->(d),\n" +
                " (b)-[:TYPE {w:1.0}]->(c),\n" +
                " (d)-[:TYPE {w:3.0}]->(c)";

        registerProcedures(KSpanningTreeProc.class);
        runQuery(cypher);
    }

    @AfterEach
    void teardownGraph() {
        db.shutdown();
    }

    private long id(String name) {
        return runQuery(
            "MATCH (n:Node) WHERE n.name = $name RETURN id(n) AS id",
            MapUtil.map("name", name),
            result -> result.<Long>columnAs("id").next()
        );
    }

    @Test
    void testMax() {
        String query = GdsCypher.call()
            .withRelationshipProperty("w")
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds.alpha.spanningTree.kmax")
            .writeMode()
            .addParameter("startNodeId", id("a"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("k", 2)
            .yields("createMillis", "computeMillis", "writeMillis");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("createMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        runQueryWithRowConsumer("MATCH (n) WHERE exists(n.partition) RETURN n.name as name, n.partition as p", row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
        });

        assertEquals(communities.get("a"), communities.get("b"));
        assertEquals(communities.get("d"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("c"));
    }

    @Test
    void testMin() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("*", Orientation.UNDIRECTED)
            .withRelationshipProperty("w")
            .algo("gds.alpha.spanningTree.kmin")
            .writeMode()
            .addParameter("startNodeId", id("a"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("k", 2)
            .yields("createMillis", "computeMillis", "writeMillis");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("createMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        runQueryWithRowConsumer("MATCH (n) WHERE exists(n.partition) RETURN n.name as name, n.partition as p", row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
        });

        assertEquals(communities.get("a"), communities.get("d"));
        assertEquals(communities.get("b"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("b"));
    }
}
