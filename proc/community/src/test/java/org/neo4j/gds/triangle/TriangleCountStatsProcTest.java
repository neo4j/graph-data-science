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
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class TriangleCountStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
                                           "(a:A)-[:T]->(b:A), " +
                                           "(b)-[:T]->(c:A), " +
                                           "(c)-[:T]->(a)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCountStatsProc.class
        );

        runQuery("CALL gds.graph.project('graph', 'A', {T: { orientation: 'UNDIRECTED'}})");
    }

    @Test
    void shouldRunStats() {
        var query = GdsCypher.call("graph")
            .algo("triangleCount")
            .statsMode()
            .yields();
        
        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("globalTriangleCount"))
                .asInstanceOf(LONG)
                .isEqualTo(1L);

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(3L);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.getNumber("postProcessingMillis"))
                .asInstanceOf(LONG)
                .isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isInstanceOf(Map.class);
        });

        assertThat(rowCount).isEqualTo(1L);

    }

    @Test
    void testStatsWithMaxDegree() {
        // Add a single node and connect it to the triangle
        // to be able to apply the maxDegree filter.
        runQuery("MATCH (n) " +
                 "WITH n LIMIT 1 " +
                 "CREATE (d)-[:REL]->(n)");

        GraphStoreCatalog.removeAllLoadedGraphs();

        loadCompleteGraph("graph", Orientation.UNDIRECTED);

        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .statsMode()
            .addParameter("maxDegree", 2)
            .yields("globalTriangleCount", "nodeCount");

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("globalTriangleCount"))
                .asInstanceOf(LONG)
                .isEqualTo(0L);

            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(4L);

        });

        assertThat(rowCount).isEqualTo(1L);

    }


}
