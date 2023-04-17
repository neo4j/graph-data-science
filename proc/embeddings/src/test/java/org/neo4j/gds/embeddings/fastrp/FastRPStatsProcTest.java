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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class FastRPStatsProcTest extends FastRPProcTest<FastRPStatsConfig> {

    @Override
    GdsCypher.ExecutionModes mode() {
        return GdsCypher.ExecutionModes.STATS;
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPStatsConfig, ?>> getProcedureClazz() {
        return FastRPStatsProc.class;
    }

    @Override
    public FastRPStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return FastRPStatsConfig.of(mapWrapper);
    }

    @Test
    void testStats() {
        runQuery(
            GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .loadEverything(Orientation.UNDIRECTED)
                .yields()
        );
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("fastRP")
            .statsMode()
            .addParameter("embeddingDimension", 2)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 5L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }
}
