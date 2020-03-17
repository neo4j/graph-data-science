/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.labelpropagation;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.WritePropertyConfigTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LabelPropagationWriteProcTest extends LabelPropagationProcTest<LabelPropagationWriteConfig> implements
    WritePropertyConfigTest<LabelPropagationWriteConfig, LabelPropagation> {

    @Override
    public Class<? extends AlgoBaseProc<?, LabelPropagation, LabelPropagationWriteConfig>> getProcedureClazz() {
        return LabelPropagationWriteProc.class;
    }

    @Override
    public LabelPropagationWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        // TODO: generalise for all WriteProcTests
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationProcTest#gdsGraphVariations")
    void testWrite(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        String writeProperty = "myFancyCommunity";
        @Language("Cypher") String query = queryBuilder
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .yields(
                "communityCount",
                "createMillis",
                "computeMillis",
                "writeMillis",
                "postProcessingMillis",
                "communityDistribution",
                "didConverge"
            );

        runQueryWithRowConsumer(query, row -> {
            long communityCount = row.getNumber("communityCount").longValue();
            long createMillis = row.getNumber("createMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            boolean didConverge = row.getBoolean("didConverge");
            Map<String, Object> communityDistribution = (Map<String, Object>) row.get("communityDistribution");
            assertEquals(10, communityCount, "wrong community count");
            assertTrue(createMillis >= 0, "invalid loadTime");
            assertTrue(writeMillis >= 0, "invalid writeTime");
            assertTrue(computeMillis >= 0, "invalid computeTime");
            assertTrue(didConverge, "did not converge");
            assertNotNull(communityDistribution);
            for (String key : Arrays.asList("min", "max", "mean", "p50", "p75", "p90", "p99", "p999", "p95")) {
                assertTrue(communityDistribution.containsKey(key));
            }
        });
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationProcTest#gdsGraphVariations")
    void respectsMaxIterations(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        @Language("Cypher") String query = queryBuilder
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "label")
            .addParameter("maxIterations", 5)
            .yields("ranIterations", "communityDistribution");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertTrue(5 >= row.getNumber("ranIterations").intValue());
                assertEquals(
                    MapUtil.map(
                        "p999", 2L,
                        "p99", 2L,
                        "p95", 2L,
                        "p90", 2L,
                        "p75", 1L,
                        "p50", 1L,
                        "min", 1L,
                        "max", 2L,
                        "mean", 1.2D
                    ),
                    row.get("communityDistribution")
                );
            }
        );

    }

    static Stream<Arguments> concurrenciesExplicitAndImplicitCreate() {
        return TestSupport.crossArguments(
            () -> Stream.of(1, 4, 8).map(Arguments::of),
            LabelPropagationProcTest::gdsGraphVariations
        );
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationProcTest#gdsGraphVariations")
    void shouldRunLabelPropagationNatural(GdsCypher.QueryBuilder queryBuilder, String desc) {

        String query = queryBuilder
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "community")
            .addParameter("seedProperty", "seed")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 8L,
                        "p99", 8L,
                        "p95", 8L,
                        "p90", 8L,
                        "p75", 8L,
                        "p50", 4L,
                        "min", 4L,
                        "max", 8L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.community AS community";
        runQueryWithRowConsumer(check, row -> {
            assertEquals(2, row.getNumber("community").intValue());
        });
    }

    @Test
    void shouldRunLabelPropagationUndirected() {
        String graphName = "myGraphUndirected";
        String writeProperty = "community";

        runQuery(graphCreateQuery(Orientation.UNDIRECTED, graphName));
        @Language("Cypher")
        String query = "CALL gds.labelPropagation.write(" +
                       "        $graph, {" +
                       "         writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQueryWithRowConsumer(query, MapUtil.map("graph", graphName, "writeProperty", writeProperty), row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 6L,
                        "p99", 6L,
                        "p95", 6L,
                        "p90", 6L,
                        "p75", 6L,
                        "p50", 6L,
                        "min", 6L,
                        "max", 6L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = String.format("MATCH (a {id: 0}), (b {id: 1}) " +
                                     "RETURN a.%1$s AS a, b.%1$s AS b", writeProperty);
        runQueryWithRowConsumer(check, row -> {
            assertEquals(2, row.getNumber("a").intValue());
            assertEquals(7, row.getNumber("b").intValue());
        });
    }

    @Test
    void shouldRunLabelPropagationReverse() {
        String writeProperty = "community";

        String query = graphCreateQuery(Orientation.REVERSE)
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .yields();

        runQueryWithRowConsumer(query,
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 6L,
                        "p99", 6L,
                        "p95", 6L,
                        "p90", 6L,
                        "p75", 6L,
                        "p50", 6L,
                        "min", 6L,
                        "max", 6L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String validateQuery = String.format(
            "MATCH (n) RETURN n.%1$s AS community, count(*) AS communitySize",
            writeProperty
        );
        assertCypherResult(validateQuery, Arrays.asList(
            MapUtil.map("community", 0L, "communitySize", 6L),
            MapUtil.map("community", 1L, "communitySize", 6L)
        ));
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("concurrenciesExplicitAndImplicitCreate")
    void shouldRunLabelPropagationWithIdenticalSeedAndWriteProperties(
        int concurrency,
        GdsCypher.QueryBuilder queryBuilder,
        String desc
    ) {

        String query = queryBuilder
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("concurrency", concurrency)
            .addParameter("writeProperty", "seed")
            .addParameter("seedProperty", "seed")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(2, row.getNumber("nodePropertiesWritten").intValue());
                assertUserInput(row, "seedProperty", "seed");
                assertUserInput(row, "writeProperty", "seed");
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 8L,
                        "p99", 8L,
                        "p95", 8L,
                        "p90", 8L,
                        "p75", 8L,
                        "p50", 4L,
                        "min", 4L,
                        "max", 8L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.seed AS community";
        runQueryWithRowConsumer(check, row -> assertEquals(2, row.getNumber("community").intValue()));
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("concurrenciesExplicitAndImplicitCreate")
    void shouldRunLabelPropagationWithoutInitialSeed(
        int concurrency,
        GdsCypher.QueryBuilder queryBuilder,
        String desc
    ) {

        String query = queryBuilder
            .algo("gds.labelPropagation")
            .writeMode()
            .addParameter("concurrency", concurrency)
            .addParameter("writeProperty", "community")
            .addParameter("nodeWeightProperty", "weight")
            .yields();

        runQueryWithRowConsumer(
            query,
            row -> {
                assertUserInput(row, "seedProperty", null);
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
            }
        );
        runQueryWithRowConsumer(
            "MATCH (n) WHERE n.id = 0 RETURN n.community AS community",
            row -> assertEquals(6, row.getNumber("community").intValue())
        );
        runQueryWithRowConsumer(
            "MATCH (n) WHERE n.id = 1 RETURN n.community AS community",
            row -> assertEquals(11, row.getNumber("community").intValue())
        );
    }

    @Test
    void testWriteEstimate() {
        String query = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .writeEstimation()
            .addAllParameters(createMinimalConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 4))).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1720L,
            "bytesMax", 2232L
        )));
    }
}
