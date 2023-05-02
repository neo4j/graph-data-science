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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

class TriangleProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER = "CREATE " +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (f:Node {name: 'f'})" +
        ", (g:Node {name: 'g'})" +
        ", (h:Node {name: 'h'})" +
        ", (i:Node {name: 'i'})" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(a)" +
        ", (c)-[:TYPE]->(h)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(f)" +
        ", (f)-[:TYPE]->(d)" +
        ", (b)-[:TYPE]->(d)" +
        ", (g)-[:TYPE]->(h)" +
        ", (h)-[:TYPE]->(i)" +
        ", (i)-[:TYPE]->(g)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(TriangleProc.class, GraphProjectProc.class);
        runQuery("CALL gds.graph.project('graph', 'Node', {TYPE: {type: 'TYPE', orientation: 'UNDIRECTED'}})");
    }

    @Test
    void testStreaming() {
        assertCypherResult(
            "CALL gds.alpha.triangles('graph', {}) " +
            "YIELD nodeA, nodeB, nodeC " +
            "RETURN nodeA + nodeB + nodeC AS triangleSum ORDER BY triangleSum", List.of(
            Map.of("triangleSum", 0L + 1L + 2L),
            Map.of("triangleSum", 3L + 4L + 5L),
            Map.of("triangleSum", 6L + 7L + 8L)
        ));
    }
}
