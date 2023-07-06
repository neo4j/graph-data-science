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
package org.neo4j.gds.paths.dag.longestPath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LongestPathStreamProcTest extends BaseProcTest {

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n0)-[:T {prop: 8.0}]->(n1)" +
        ", (n0)-[:T {prop: 5.0}]->(n2)" +
        ", (n2)-[:T {prop: 2.0}]->(n1)" +
        ", (n1)-[:T {prop: 0.0}]->(n3)" +
        ", (n2)-[:T {prop: 4.0}]->(n3)";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            LongestPathStreamProc.class
        );

        var projectQuery = GdsCypher.call("last")
            .graphProject()
            .withRelationshipProperty("prop")
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
    }

    @Test
    void testStreamWithWeights() {
        String query = GdsCypher.call("last")
            .algo("gds.alpha.longestPath")
            .streamMode()
            .addParameter("computeLongestPathDistances", true)
            .addParameter("relationshipWeightProperty", "prop")
            .yields();

        runQueryWithResultConsumer(query, result -> {
            var record = result.next();
            assertEquals(idFunction.of("n0"), record.get("nodeId"));
            assertEquals(0.0, record.get("distance"));
            record = result.next();
            assertEquals(idFunction.of("n2"), record.get("nodeId"));
            assertEquals(5.0, record.get("distance"));
            record = result.next();
            assertEquals(idFunction.of("n1"), record.get("nodeId"));
            assertEquals(8.0, record.get("distance"));
            record = result.next();
            assertEquals(idFunction.of("n3"), record.get("nodeId"));
            assertEquals(9.0, record.get("distance"));
            assertFalse(result.hasNext());
        });
    }
}
