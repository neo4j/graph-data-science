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
package org.neo4j.gds.labelpropagation;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;
import org.neo4j.graphdb.Result;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class LabelPropagationWriteProcTest extends LabelPropagationProcTest<LabelPropagationWriteConfig> {

    @Override
    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<LabelPropagation, LabelPropagationResult, LabelPropagationWriteConfig, ?>> getProcedureClazz() {
        return LabelPropagationWriteProc.class;
    }

    @Override
    public LabelPropagationWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        // TODO: generalise for all WriteProcTests
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    @Test
    void testWrite() {
        String writeProperty = "myFancyCommunity";
        @Language("Cypher") String query = GdsCypher.call(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .yields(
                "communityCount",
                "preProcessingMillis",
                "computeMillis",
                "writeMillis",
                "postProcessingMillis",
                "communityDistribution",
                "didConverge"
            );

        runQueryWithRowConsumer(query, row -> {
            long communityCount = row.getNumber("communityCount").longValue();
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();
            boolean didConverge = row.getBoolean("didConverge");
            Map<String, Object> communityDistribution = (Map<String, Object>) row.get("communityDistribution");
            assertEquals(10, communityCount, "wrong community count");
            assertTrue(preProcessingMillis >= 0, "invalid preProcessingTime");
            assertTrue(writeMillis >= 0, "invalid writeTime");
            assertTrue(computeMillis >= 0, "invalid computeTime");
            assertTrue(didConverge, "did not converge");
            assertNotNull(communityDistribution);
            for (String key : Arrays.asList("min", "max", "mean", "p50", "p75", "p90", "p99", "p999", "p95")) {
                assertTrue(communityDistribution.containsKey(key));
            }
        });
    }

    @Test
    void respectsMaxIterations() {
        @Language("Cypher") String query = GdsCypher.call(TEST_GRAPH_NAME)
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

    static Stream<Arguments> concurrencies() {
        return TestSupport.crossArguments(
            () -> Stream.of(1, 2, 4).map(Arguments::of)
        );
    }

    @Test
    void shouldRunLabelPropagationNatural() {

        String query = GdsCypher.call(TEST_GRAPH_NAME)
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

        runQuery(graphProjectQuery(Orientation.UNDIRECTED, graphName));
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
        String check = formatWithLocale("MATCH (a {id: 0}), (b {id: 1}) " +
                                     "RETURN a.%1$s AS a, b.%1$s AS b", writeProperty);
        runQueryWithRowConsumer(check, row -> {
            assertEquals(2, row.getNumber("a").intValue());
            assertEquals(7, row.getNumber("b").intValue());
        });
    }

    @Test
    void shouldRunLabelPropagationReverse() {
        String writeProperty = "community";

        var createQuery = graphProjectQuery(DEFAULT_GRAPH_NAME, Orientation.REVERSE).yields();
        runQuery(createQuery);

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
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
        String validateQuery = formatWithLocale(
            "MATCH (n) RETURN n.%1$s AS community, count(*) AS communitySize",
            writeProperty
        );
        assertCypherResult(validateQuery, Arrays.asList(
            MapUtil.map("community", 0L, "communitySize", 6L),
            MapUtil.map("community", 1L, "communitySize", 6L)
        ));
    }

    @ParameterizedTest(name = "concurrency = {0}")
    @MethodSource("concurrencies")
    void shouldRunLabelPropagationWithIdenticalSeedAndWriteProperties(int concurrency) {

        String query = GdsCypher.call(TEST_GRAPH_NAME)
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

    @ParameterizedTest(name = "concurrency = {0}")
    @MethodSource("concurrencies")
    void shouldRunLabelPropagationWithoutInitialSeed(int concurrency) {

        String query = GdsCypher.call(TEST_GRAPH_NAME)
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

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), new Long[] {2L, 7L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L}),
            Arguments.of(Map.of("minCommunitySize", 2), new Long[] {2L, 7L}),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), new Long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}),
            Arguments.of(Map.of("minCommunitySize", 2, "consecutiveIds", true), new Long[] {0L, 1L}),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", "seed"), new Long[] {1L, 2L}),
            Arguments.of(Map.of("minCommunitySize", 2, "seedProperty", "seed"), new Long[] {1L, 2L})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, Long[] expectedCommunityIds) {
        String writeProp = "writeProp";
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty("seed")
            .yields();
        runQuery(createQuery);

        var query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", writeProp)
            .addAllParameters(parameters)
            .yields("communityCount");

        runQueryWithRowConsumer(
            query,
            row -> assertEquals(parameters.containsKey("seedProperty") ? 2L : 10L, row.getNumber("communityCount"))
        );

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + writeProp + ") AS communities ",
            row -> {
                @SuppressWarnings("unchecked") var actualComponents = (List<Long>) row.get("communities");
                assertThat(actualComponents, containsInAnyOrder(expectedCommunityIds));
            }
        );
    }

    @Test
    void testWriteEstimate() {
        String query = GdsCypher
            .call(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .writeEstimation()
            .addAllParameters(createMinimalConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 4))).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1640L,
            "bytesMax", 2152L
        )));
    }

    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");

        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .yields();
        runQuery(createQuery);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("labelPropagation")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    @Nested
    class FilteredGraph extends BaseTest {

        @Neo4jGraph
        static final String DB_CYPHER_WITH_OFFSET = "CREATE (c:Ignore {id:12, seed: 0}) " + DB_CYPHER + " CREATE (a)-[:X]->(c), (c)-[:X]->(b)";

        @Test
        void shouldRunLabelPropagationNaturalOnFilteredNodes() {
            String graphCreateQuery = GdsCypher
                .call("nodeFilterGraph")
                .graphProject()
                .withNodeLabels("A", "B")
                .withNodeProperty("id", DefaultValue.of(-1))
                .withNodeProperty("seed", DefaultValue.of(Long.MIN_VALUE))
                .withNodeProperty("weight", DefaultValue.of(Double.NaN))
                .withAnyRelationshipType()
                .yields("nodeCount", "relationshipCount");

            runQueryWithRowConsumer(graphCreateQuery, row -> {
                assertEquals(12L, row.getNumber("nodeCount"));
                assertEquals(10L, row.getNumber("relationshipCount"));
            });

            String query = GdsCypher.call("nodeFilterGraph")
                .algo("gds.labelPropagation")
                .writeMode()
                .addParameter("writeProperty", "community")
                .addParameter("seedProperty", "seed")
                .addParameter("nodeWeightProperty", "weight")
                .addParameter("nodeLabels", Arrays.asList("A", "B"))
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
    }

    private void checkMillisSet(Result.ResultRow row) {
        assertTrue(row.getNumber("preProcessingMillis").intValue() >= 0, "load time not set");
        assertTrue(row.getNumber("computeMillis").intValue() >= 0, "compute time not set");
        assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
    }
}
