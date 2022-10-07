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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ClosenessCentralityMutateProcTest extends ClosenessCentralityProcTest<ClosenessCentralityMutateConfig> {

    private static final String MUTATE_PROPERTY = "score";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphWriteNodePropertiesProc.class, GraphStreamNodePropertiesProc.class);
    }

    @Override
    public Class<? extends AlgoBaseProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig, ?>> getProcedureClazz() {
        return ClosenessCentralityMutateProc.class;
    }

    @Override
    public ClosenessCentralityMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return ClosenessCentralityMutateConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateProperty")) {
            return mapWrapper.withString("mutateProperty", MUTATE_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testClosenessMutate() {
        loadGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("mutateMillis")).isNotEqualTo(-1L);
            assertThat(row.getNumber("preProcessingMillis")).isNotEqualTo(-1L);
            assertThat(row.getNumber("computeMillis")).isNotEqualTo(-1L);
            assertThat(row.getNumber("nodePropertiesWritten")).isEqualTo(11L);

            assertThat(row.get("configuration")).isNotNull();

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

        assertCypherResult(
            formatWithLocale("MATCH (n) WHERE n.%s IS NOT NULL RETURN count(n) AS count", MUTATE_PROPERTY),
            List.of(Map.of("count", 0L))
        );

        assertCypherResult(
            formatWithLocale(
                "CALL gds.graph.nodeProperties.stream('graph',['%1$s']) YIELD nodeId, propertyValue AS %1$s",
                MUTATE_PROPERTY
            ),
            expectedCentralityResult
        );
    }
}
