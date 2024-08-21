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
package org.neo4j.gds.articulationpoints;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.registerProcedures;

class ArticulationPointsStatsProcTest  extends BaseTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        """
            CREATE
               (a1:Node),
               (a2:Node),
               (a3:Node),
               (a4:Node),
               (a5:Node),
               (a6:Node),
               (a7:Node),
               (a8:Node),
               (a9:Node),
               (a10:Node),
               (a11:Node),
               (a12:Node),
               (a13:Node),
               (a14:Node),
               (a15:Node),
               (a16:Node),
               (a1)-[:R]->(a2),
               (a3)-[:R]->(a4),
               (a3)-[:R]->(a7),
               (a7)-[:R]->(a8),
               (a5)-[:R]->(a9),
               (a5)-[:R]->(a10),
               (a9)-[:R]->(a10),
               (a9)-[:R]->(a14),
               (a10)-[:R]->(a11),
               (a11)-[:R]->(a12),
               (a10)-[:R]->(a14),
               (a11)-[:R]->(a15),
               (a12)-[:R]->(a16),
               (a13)-[:R]->(a14),
               (a15)-[:R]->(a16)
            """;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            db,
            ArticulationPointsStatsProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'Node', {R: {orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldComputeStats() {

        var resultRowCount = runQueryWithRowConsumer(
            "CALL gds.articulationPoints.stats('graph')",
            row -> {
                var nodeId = row.getNumber("articulationPointCount");
                assertThat(nodeId)
                    .asInstanceOf(LONG)
                    .isEqualTo(5L);
            }
        );

        assertThat(resultRowCount)
            .isEqualTo(1L);

    }

    @Test
    void shouldEstimateStats() {
        var resultRowCount = runQueryWithRowConsumer(
            "CALL gds.articulationPoints.stats.estimate('graph',{})",
            row -> {
                assertThat(row.get("requiredMemory")).isNotNull();
                assertThat(row.get("treeView")).isNotNull();
            }
        );

        assertThat(resultRowCount)
            .as("There should be one row as a result of estimating write.")
            .isEqualTo(1L);
    }

}
