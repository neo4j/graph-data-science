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
package org.neo4j.gds.paths.dag.topologicalsort;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TopologicalSortStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-[:R]->(n1)" +
        ", (n0)-[:R]->(n2)" +
        ", (n2)-[:R]->(n1)" +
        ", (n3)-[:R]->(n0)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TopologicalSortStreamProc.class
        );

        var projectQuery = GdsCypher.call("last").graphProject().loadEverything(Orientation.NATURAL).yields();
        runQuery(projectQuery);
    }

    @Test
    void testStream() {
        String query = GdsCypher.call("last")
            .algo("gds.dag.topologicalSort")
            .streamMode()
            .yields();

        runQueryWithResultConsumer(query, result -> {
            var record = result.next();
            assertEquals(idFunction.of("n3"), record.get("nodeId"));
            assertEquals(null, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n0"), record.get("nodeId"));
            assertEquals(null, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n2"), record.get("nodeId"));
            assertEquals(null, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n1"), record.get("nodeId"));
            assertEquals(null, record.get("maxDistanceFromSource"));
            assertFalse(result.hasNext());
        });
    }

    @Test
    void testStreamWithLongestPathDistances() {
        String query = GdsCypher.call("last")
            .algo("gds.dag.topologicalSort")
            .streamMode()
            .addParameter("computeMaxDistanceFromSource", true)
            .yields();

        runQueryWithResultConsumer(query, result -> {
            var record = result.next();
            assertEquals(idFunction.of("n3"), record.get("nodeId"));
            assertEquals(0.0, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n0"), record.get("nodeId"));
            assertEquals(1.0, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n2"), record.get("nodeId"));
            assertEquals(2.0, record.get("maxDistanceFromSource"));
            record = result.next();
            assertEquals(idFunction.of("n1"), record.get("nodeId"));
            assertEquals(3.0, record.get("maxDistanceFromSource"));
            assertFalse(result.hasNext());
        });
    }
}
