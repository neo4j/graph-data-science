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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class TriangleCentralityDistributionProcTest extends BaseProcTest {

    // K4, a leaf attached to a, and an isolated node. Interleave labels to exercise filtered IDs.
    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE " +
        "(isolated:A:Outside), (a:A:Clique), (leaf:A:Outside), (b:A:Clique), (c:A:Clique), (d:A:Clique), " +
        "(a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), " +
        "(b)-[:T]->(c), (b)-[:T]->(d), (c)-[:T]->(d), (a)-[:T]->(leaf)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class, TriangleCentralityStatsProc.class,
            TriangleCentralityMutateProc.class, TriangleCentralityWriteProc.class
        );
        runQuery("CALL gds.graph.project('graph', ['A', 'Clique', 'Outside'], {T: {orientation: 'UNDIRECTED'}})");
    }

    private static Stream<Arguments> distributions() {
        return Stream.of("stats", "mutate", "write").flatMap(mode -> Stream.of(1, 4).flatMap(concurrency ->
            Stream.of(Long.MAX_VALUE, 3L).map(maxDegree -> Arguments.of(mode, concurrency, maxDegree))
        ));
    }

    @ParameterizedTest
    @MethodSource("distributions")
    void shouldSummarizeIncludedScores(String mode, int concurrency, long maxDegree) {
        // Without a cutoff the scores sum to 19/4 across six nodes. With maxDegree=3, a is
        // excluded and the surviving triangle gives three unit scores among five included nodes.
        // The mean must omit the excluded NaN while retaining the leaf's and isolated node's zeros.
        var config = configurationForMode(mode);
        config.put("concurrency", concurrency);
        config.put("maxDegree", maxDegree);
        var rows = runQueryWithRowConsumer(db, query(mode, "graph"), Map.of("config", config), row -> {
            var distribution = distribution(row.get("centralityDistribution"));
            assertThat(distribution).containsOnlyKeys("min", "max", "mean", "p50", "p75", "p90", "p95", "p99", "p999");
            assertThat((double) distribution.get("min")).isZero();
            assertThat((double) distribution.get("max")).isCloseTo(1.0, within(1e-5));
            assertThat((double) distribution.get("mean"))
                .isCloseTo(maxDegree == Long.MAX_VALUE ? 19.0 / 24 : 0.6, within(1e-5));
            for (String percentile : List.of("p50", "p75", "p90", "p95", "p99", "p999")) {
                assertThat((double) distribution.get(percentile)).isCloseTo(1.0, within(1e-5));
            }
            assertThat(row.getNumber("postProcessingMillis").longValue()).isNotNegative();
        });
        assertThat(rows).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"stats", "mutate", "write"})
    void shouldReturnEmptyDistributionWhenAllNodesAreExcluded(String mode) {
        var config = configurationForMode(mode);
        config.put("nodeLabels", List.of("Clique"));
        config.put("maxDegree", 2L);
        var rows = runQueryWithRowConsumer(db, query(mode, "graph"), Map.of("config", config), row -> {
            assertThat(distribution(row.get("centralityDistribution"))).isEmpty();
            assertThat(row.getNumber("postProcessingMillis").longValue()).isNotNegative();
        });
        assertThat(rows).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"stats", "mutate", "write"})
    void shouldIncludeZeroScoresWhenThereAreNoTriangles(String mode) {
        var config = configurationForMode(mode);
        config.put("nodeLabels", List.of("Outside"));
        var rows = runQueryWithRowConsumer(db, query(mode, "graph"), Map.of("config", config), row -> {
            var distribution = distribution(row.get("centralityDistribution"));
            assertThat(distribution).hasSize(9);
            assertThat(distribution.values()).allMatch(value -> ((Number) value).doubleValue() == 0.0);
        });
        assertThat(rows).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"stats", "mutate", "write"})
    void shouldReturnEmptyDistributionForEmptyGraph(String mode) {
        runQuery("CALL db.createLabel('Empty')");
        runQuery("CALL db.createRelationshipType('EMPTY')");
        runQuery("CALL gds.graph.project('emptyGraph', 'Empty', {EMPTY: {orientation: 'UNDIRECTED'}})");
        var rows = runQueryWithRowConsumer(db, query(mode, "emptyGraph"), Map.of("config", configurationForMode(mode)), row -> {
            assertThat(distribution(row.get("centralityDistribution"))).isEmpty();
            assertThat(row.getNumber("postProcessingMillis").longValue()).isZero();
        });
        assertThat(rows).isEqualTo(1);
    }

    private static String query(String mode, String graph) {
        return "CALL gds.triangleCentrality." + mode + "('" + graph + "', $config) " +
            "YIELD centralityDistribution, postProcessingMillis";
    }

    private static Map<String, Object> configurationForMode(String mode) {
        var config = new HashMap<String, Object>();
        if (mode.equals("mutate")) {
            config.put("mutateProperty", "centrality");
        }
        if (mode.equals("write")) {
            config.put("writeProperty", "centrality");
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> distribution(Object value) {
        assertThat(value).isInstanceOf(Map.class);
        return (Map<String, Object>) value;
    }
}
