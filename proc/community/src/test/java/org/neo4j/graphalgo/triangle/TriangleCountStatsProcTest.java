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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TriangleCountStatsProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            TriangleCountStatsProc.class
        );

        var DB_CYPHER = "CREATE " +
                        "(a:A)-[:T]->(b:A), " +
                        "(b)-[:T]->(c:A), " +
                        "(c)-[:T]->(a)";

        runQuery(DB_CYPHER);
        runQuery("CALL gds.graph.create('g', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStats() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("triangleCount")
            .statsMode()
            .addParameter("sudo", true)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            var createMillis = row.getNumber("createMillis").longValue();
            var computeMillis = row.getNumber("computeMillis").longValue();
            var nodeCount = row.getNumber("nodeCount").longValue();
            var triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, createMillis);
            assertNotEquals(-1, computeMillis);
            assertEquals(1, triangleCount);
            assertEquals(3, nodeCount);
        });
    }

}