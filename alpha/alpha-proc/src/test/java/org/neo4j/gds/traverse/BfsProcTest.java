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
package org.neo4j.gds.traverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.compat.MapUtil.map;

/**
 * Graph:
 *
 *     (b)   (e)
 *   2/ 1\ 2/ 1\
 * >(a)  (d)  ((g))
 *   1\ 2/ 1\ 2/
 *    (c)   (f)
 */
class BfsProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name:'a'})" +
        ", (b:Node {name:'b'})" +
        ", (c:Node {name:'c'})" +
        ", (d:Node {name:'d'})" +
        ", (e:Node {name:'e'})" +
        ", (f:Node {name:'f'})" +
        ", (g:Node {name:'g'})" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE2]->(a)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(BfsStreamProc.class, GraphProjectProc.class);
        runQuery(DB_CYPHER);
    }

    private long id(String name) {
        return runQuery("MATCH (n:Node) WHERE n.name = $name RETURN id(n) AS id", map("name", name), result -> result.<Long>columnAs("id").next());
    }

    void assertOrder(Map<String, List<Integer>> expected, List<Long> nodeIds) {
        assertEquals(expected.size(), nodeIds.size(), "expected " + expected + " | given [" + nodeIds + "]");
        for (var ex : expected.entrySet()) {
            long id = id(ex.getKey());
            var expectedPositions = ex.getValue();
            int actualPosition = nodeIds.indexOf(id);
            if (!expectedPositions.contains(actualPosition)) {
                fail(ex.getKey() + "(" + id + ") at " + actualPosition + " expected at " + expectedPositions);
            }
        }
    }

    @Test
    void testBfsPath() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.REVERSE)
            .yields();
        runQuery(createQuery);

        long id = id("g");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.bfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertEquals(row.getNumber("sourceNode").longValue(), id);
            List<Long> nodeIds = (List<Long>) row.get("nodeIds");
            var expectedOrder = new HashMap<String, List<Integer>>();
            expectedOrder.put("g", List.of(0));
            expectedOrder.put("f", List.of(1, 2));
            expectedOrder.put("e", List.of(1, 2));
            expectedOrder.put("d", List.of(3));
            expectedOrder.put("c", List.of(4, 5));
            expectedOrder.put("b", List.of(4, 5));
            expectedOrder.put("a", List.of(6));

            assertOrder(expectedOrder, nodeIds);
        });
    }

    @Test
    void worksOnGraphWithLoop() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        long id = id("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.bfs")
            .streamMode()
            .addParameter("sourceNode", id)
            .yields("sourceNode, nodeIds");
        runQueryWithRowConsumer(query, row -> {
            assertEquals(row.getNumber("sourceNode").longValue(), id);
            List<Long> nodeIds = (List<Long>) row.get("nodeIds");
            var expectedOrder = new HashMap<String, List<Integer>>();
            expectedOrder.put("g", List.of(6));
            expectedOrder.put("f", List.of(4, 5));
            expectedOrder.put("e", List.of(4, 5));
            expectedOrder.put("d", List.of(3));
            expectedOrder.put("c", List.of(1, 2));
            expectedOrder.put("b", List.of(1, 2));
            expectedOrder.put("a", List.of(0));

            assertOrder(expectedOrder, nodeIds);
        });
    }
}
