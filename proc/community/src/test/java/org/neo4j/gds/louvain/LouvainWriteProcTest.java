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
package org.neo4j.gds.louvain;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG_ARRAY;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class LouvainWriteProcTest extends LouvainProcTest<LouvainWriteConfig> {

    @Override
    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<Louvain, LouvainResult, LouvainWriteConfig, ?>> getProcedureClazz() {
        return LouvainWriteProc.class;
    }

    @Test
    void testWrite() {
        String writeProperty = "myFancyCommunity";
        var query = GdsCypher.call("myGraph")
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .yields(
                "communityCount",
                "modularity",
                "modularities",
                "ranLevels",
                "preProcessingMillis",
                "computeMillis",
                "writeMillis",
                "postProcessingMillis",
                "communityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .as("wrong community count")
                .isEqualTo(3L);

            assertThat(row.get("modularities"))
                .asList()
                .as("invalud modularities")
                .hasSize(2);

            assertThat(row.getNumber("ranLevels"))
                .asInstanceOf(LONG)
                .as("invalid level count")
                .isEqualTo(2L);

            assertUserInput(row, "includeIntermediateCommunities", false);

            assertThat(row.getNumber("modularity"))
                .asInstanceOf(DOUBLE)
                .as("wrong modularity value")
                .isGreaterThan(0);

            assertThat(row.getNumber("preProcessingMillis"))
                .asInstanceOf(LONG)
                .as("invalid preProcessingTime")
                .isGreaterThanOrEqualTo(0);

            assertThat(row.getNumber("writeMillis"))
                .asInstanceOf(LONG)
                .as("invalid writeTime")
                .isGreaterThanOrEqualTo(0);

            assertThat(row.getNumber("computeMillis"))
                .asInstanceOf(LONG)
                .as("invalid computeTime")
                .isGreaterThanOrEqualTo(0);
        });
        assertWriteResult(RESULT, writeProperty);
    }

    @Test
    void testWriteIntermediateCommunities() {
        String writeProperty = "myFancyCommunity";
        String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .addParameter("includeIntermediateCommunities", true)
            .yields("configuration");

        runQueryWithRowConsumer(query, row -> {
            assertUserInput(row, "includeIntermediateCommunities", true);
        });

        runQueryWithRowConsumer(formatWithLocale("MATCH (n) RETURN n.%s as %s", writeProperty, writeProperty), row -> {
            assertThat(row.get(writeProperty))
                .asInstanceOf(LONG_ARRAY)
                .hasSize(2);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "writeProperty: null",
        "writeProperty: ''",
    })
    void testWriteRequiresWritePropertyToBeSet(String writePropertyParameter) {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE", Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
        String query = "CALL gds.louvain.write('" + DEFAULT_GRAPH_NAME + "', {" +
                       writePropertyParameter +
                       "})";

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery(query))
            .havingRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .withMessage("No value specified for the mandatory configuration parameter `writeProperty`");
    }

    @Test
    void testWriteWithSeeding() {
        String writeProperty = "myFancyWriteProperty";
        String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .addParameter("seedProperty", SEED_PROPERTY)
            .yields("communityCount", "ranLevels");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(row.getNumber("communityCount"))
                    .asInstanceOf(LONG)
                    .as("wrong community count")
                    .isEqualTo(3L);
                assertThat(row.getNumber("ranLevels"))
                    .asInstanceOf(LONG)
                    .as("wrong number of levels")
                    .isEqualTo(1L);
            }
        );
        assertWriteResult(RESULT, writeProperty);
    }

    // TODO: add a variation of the below with non-defaults
    @Test
    void testCreateConfigWithDefaults() {
        LouvainBaseConfig louvainConfig = LouvainWriteConfig.of(createMinimalConfig(CypherMapWrapper.empty()));
        assertThat(louvainConfig.includeIntermediateCommunities()).isFalse();
        assertThat(louvainConfig.maxLevels()).isEqualTo(10);
        assertThat(louvainConfig.maxIterations()).isEqualTo(10);
        assertThat(louvainConfig.tolerance()).isEqualTo(0.0001D);
        assertThat(louvainConfig.seedProperty()).isNull();
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
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            // configuration | expectedCommunityCount | expectedCommunityIds
            Arguments.of(Map.of("minCommunitySize", 1), 3L, new Long[]{11L, 13L, 14L}),
            Arguments.of(Map.of("minCommunitySize", 3), 3L, new Long[]{14L, 13L}),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), 3L, new Long[]{0L, 1L, 2L}),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), 3L, new Long[]{0L, 1L}),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", SEED_PROPERTY), 3L, new Long[]{1L, 2L, 42L}),
            Arguments.of(Map.of("minCommunitySize", 3, "seedProperty", SEED_PROPERTY), 3L, new Long[]{2L, 42L})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, long expectedCommunityCount, Long[] expectedCommunityIds) {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty(SEED_PROPERTY)
            .yields();
        runQuery(createQuery);

        var query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addAllParameters(parameters)
            .yields("communityCount");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(expectedCommunityCount);
        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + WRITE_PROPERTY + ") AS communities ",
            row -> {
                assertThat(row.get("communities"))
                    .asList()
                    .containsExactlyInAnyOrder(expectedCommunityIds);
            }
        );
    }

    @Override
    public LouvainWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    private void assertWriteResult(List<List<Long>> expectedCommunities, String writeProperty) {
        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(formatWithLocale("MATCH (n) RETURN id(n) as id, n.%s as community", writeProperty), (row) -> {
            long community = row.getNumber("community").longValue();
            int id = row.getNumber("id").intValue();
            actualCommunities.add(id, community);
        });

        assertCommunities(actualCommunities, expectedCommunities);
    }
}
