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
package org.neo4j.gds.similarity.knn;

import org.assertj.core.api.Condition;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.SimilarityStatsResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class KnnStatsProcTest extends KnnProcTest<SimilarityStatsResult, KnnStatsConfig> {

    private static Stream<Arguments> negativeGraphs() {
        return Stream.of(
            Arguments.of("CREATE ({weight: [1.0, 2.0]}), ({weight: [3.0, -10.0]})", "negative float arrays"),
            Arguments.of("CREATE ({weight: [1.0D, 2.0D]}), ({weight: [3.0D, -10.0D]})", "negative double arrays"),
            Arguments.of("CREATE ({weight: -99}), ({weight: -10})", "negative long values")
        );
    }

    @Test
    void testStatsYields() {
        String query = GdsCypher
            .call()
            .withNodeProperty("knn")
            .loadEverything()
            .algo("gds", "beta", "knn")
            .statsMode()
            .addParameter("sudo", true)
            .addParameter("nodeWeightProperty", "knn")
            .addParameter("topK", 1)
            .yields(
                "createMillis",
                "computeMillis",
                "postProcessingMillis",
                "nodesCompared",
                "similarityPairs",
                "similarityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3, row.getNumber("nodesCompared").longValue());
            assertEquals(3, row.getNumber("similarityPairs").longValue());

            assertThat(row)
                .extracting(r -> r.getNumber("computeMillis"), InstanceOfAssertFactories.LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row)
                .extracting(r -> r.getNumber("createMillis"), InstanceOfAssertFactories.LONG)
                .isGreaterThanOrEqualTo(0L);

            assertThat(row)
                .extracting(r -> r.getNumber("postProcessingMillis"), InstanceOfAssertFactories.LONG)
                .isGreaterThanOrEqualTo(0L);

            var assertAsMap = InstanceOfAssertFactories.map(String.class, Double.class);
            var positiveDouble = new Condition<Double>((v) -> v >= 0, "a positive value");
            assertThat(row)
                .extracting(r -> r.get("similarityDistribution"), assertAsMap)
                .hasEntrySatisfying("min", positiveDouble)
                .hasEntrySatisfying("max", positiveDouble)
                .hasEntrySatisfying("mean", positiveDouble)
                .hasEntrySatisfying("stdDev", positiveDouble)
                .hasEntrySatisfying("p1", positiveDouble)
                .hasEntrySatisfying("p5", positiveDouble)
                .hasEntrySatisfying("p10", positiveDouble)
                .hasEntrySatisfying("p25", positiveDouble)
                .hasEntrySatisfying("p50", positiveDouble)
                .hasEntrySatisfying("p75", positiveDouble)
                .hasEntrySatisfying("p90", positiveDouble)
                .hasEntrySatisfying("p95", positiveDouble)
                .hasEntrySatisfying("p99", positiveDouble)
                .hasEntrySatisfying("p100", positiveDouble);
        });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.similarity.knn.KnnProcTest#allGraphVariations")
    void statsShouldNotHaveWriteProperties(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .algo("gds", "beta", "knn")
            .statsMode()
            .addParameter("nodeWeightProperty", "knn")
            .yields();

        List<String> forbiddenResultColumns = List.of(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = List.of(
            "writeProperty",
            "writeRelationshipType"
        );
        runQueryWithResultConsumer(query, result -> {
            List<String> badResultColumns = result.columns()
                .stream()
                .filter(forbiddenResultColumns::contains)
                .collect(Collectors.toList());
            assertEquals(List.of(), badResultColumns);
            assertTrue(result.hasNext(), "Result must not be empty.");
            Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
            List<String> badConfigKeys = config.keySet()
                .stream()
                .filter(forbiddenConfigKeys::contains)
                .collect(Collectors.toList());
            assertEquals(List.of(), badConfigKeys);
        });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("negativeGraphs")
    void supportNegativeArrays(String graphCreateQuery, String desc) {
        clearDb();

        runQuery(graphCreateQuery);
        runQuery("CALL gds.graph.create('graph', '*', '*', {nodeProperties: 'weight'})");

        String algoQuery = GdsCypher
            .call()
            .explicitCreation("graph")
            .algo("gds.beta.knn")
            .statsMode()
            .addParameter("nodeWeightProperty", "weight")
            .addParameter("randomSeed", 42)
            .yields("similarityPairs");

        assertCypherResult(algoQuery, List.of(Map.of("similarityPairs", 2L)));
    }

    @Override
    public Class<? extends AlgoBaseProc<Knn, Knn.Result, SimilarityStatsResult, KnnStatsConfig>> getProcedureClazz() {
        return KnnStatsProc.class;
    }

    @Override
    public KnnStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return KnnStatsConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
