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
package org.neo4j.gds.beta.closeness;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ClosenessCentralityStatsProcTest extends ClosenessCentralityProcTest<ClosenessCentralityStatsConfig> {
    @Override
    public Class<? extends AlgoBaseProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStatsConfig, ?>> getProcedureClazz() {
        return ClosenessCentralityStatsProc.class;
    }

    @Override
    public ClosenessCentralityStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return ClosenessCentralityStatsConfig.of(mapWrapper);
    }

    @Test
    void testStats() {
        loadGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .statsMode()
            .yields("centralityDistribution", "preProcessingMillis", "computeMillis", "postProcessingMillis", "configuration");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThan(-1L);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThan(-1L);
            assertThat(row.getNumber("postProcessingMillis").longValue()).isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isNotNull()
                .isInstanceOf(Map.class);

            assertThat(row.get("centralityDistribution")).isEqualTo(Map.of(
                "max", 1.0000038146972656,
                "mean", 0.6256675720214844,
                "min", 0.5882339477539062,
                "p50", 0.5882339477539062,
                "p75", 0.5882339477539062,
                "p90", 0.5882339477539062,
                "p95", 1.0000038146972656,
                "p99", 1.0000038146972656,
                "p999", 1.0000038146972656
            ));

        });
    }
}
