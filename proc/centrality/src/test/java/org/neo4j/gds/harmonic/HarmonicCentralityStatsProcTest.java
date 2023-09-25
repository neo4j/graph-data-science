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
package org.neo4j.gds.harmonic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HarmonicCentralityStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "  (a:Node {name:'a'})" +
        ", (b:Node {name:'b'})" +
        ", (c:Node {name:'c'})" +
        ", (d:Node {name:'d'})" +
        ", (e:Node {name:'e'})" +
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (d)-[:TYPE]->(e)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(HarmonicCentralityStatsProc.class, GraphProjectProc.class);
        runQuery("CALL gds.graph.project('graph', '*', '*')");
    }

    @Test
    void shouldMutate() {
        var query = "CALL gds.closeness.harmonic.stats('graph', {})";

        var rowCount = runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThan(-1L);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThan(-1L);
            assertThat(row.getNumber("postProcessingMillis").longValue()).isGreaterThan(-1L);

            assertThat(row.get("configuration"))
                .isNotNull()
                .isInstanceOf(Map.class);

            assertThat(row.get("centralityDistribution")).isEqualTo(Map.of(
                "max", 0.375,
                "mean", 0.175,
                "min", 0.0,
                "p50", 0.25,
                "p75", 0.25,
                "p90", 0.375,
                "p95", 0.375,
                "p99", 0.375,
                "p999", 0.375
            ));
        });

        assertThat(rowCount)
            .as("`stats` mode should always return one row")
            .isEqualTo(1L);
    }

}
