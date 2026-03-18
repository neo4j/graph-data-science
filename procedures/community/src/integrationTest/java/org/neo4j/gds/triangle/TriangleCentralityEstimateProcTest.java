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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class TriangleCentralityEstimateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "(a:A)-[:T]->(b:A), (b)-[:T]->(c:A), (c)-[:T]->(a), (a)-[:T]->(d:A)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            TriangleCentralityStreamProc.class,
            TriangleCentralityStatsProc.class,
            TriangleCentralityMutateProc.class,
            TriangleCentralityWriteProc.class
        );
        runQuery("CALL gds.graph.project('graph', 'A', {T: {orientation: 'UNDIRECTED'}})");
    }

    @ParameterizedTest
    @ValueSource(strings = {"stream", "stats", "mutate", "write"})
    void shouldEstimateMemory(String mode) {
        Map<String, Object> config = switch (mode) {
            case "mutate" -> Map.of("mutateProperty", "centrality");
            case "write" -> Map.of("writeProperty", "centrality");
            default -> Map.of();
        };

        runQueryWithRowConsumer(
            "CALL gds.triangleCentrality." + mode + ".estimate('graph', $config)", Map.of("config", config), row -> {
                assertThat(row.getNumber("nodeCount")).asInstanceOf(LONG).isEqualTo(4);
                assertThat(row.getNumber("relationshipCount")).asInstanceOf(LONG).isEqualTo(8);
                assertThat(row.getNumber("bytesMin")).asInstanceOf(LONG).isPositive();
                assertThat(row.getNumber("bytesMax")).asInstanceOf(LONG)
                    .isGreaterThanOrEqualTo(row.getNumber("bytesMin").longValue());
            }
        );
    }
}
