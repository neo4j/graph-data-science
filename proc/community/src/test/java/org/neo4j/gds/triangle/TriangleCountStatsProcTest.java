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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class TriangleCountStatsProcTest extends TriangleCountBaseProcTest<TriangleCountStatsConfig> {

    @Test
    void testStats() {
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 1L,
            "nodeCount", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void testStatsWithMaxDegree() {
        // Add a single node and connect it to the triangle
        // to be able to apply the maxDegree filter.
        runQuery("MATCH (n) " +
                 "WITH n LIMIT 1 " +
                 "CREATE (d)-[:REL]->(n)");

        GraphStoreCatalog.removeAllLoadedGraphs();
        loadGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);

        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .statsMode()
            .addParameter("maxDegree", 2)
            .yields("globalTriangleCount", "nodeCount");

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 0L,
            "nodeCount", 4L
        )));
    }

    @Override
    public Class<? extends AlgoBaseProc<IntersectingTriangleCount, TriangleCountResult, TriangleCountStatsConfig, ?>> getProcedureClazz() {
        return TriangleCountStatsProc.class;
    }

    @Override
    public TriangleCountStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return TriangleCountStatsConfig.of(mapWrapper);
    }

}
