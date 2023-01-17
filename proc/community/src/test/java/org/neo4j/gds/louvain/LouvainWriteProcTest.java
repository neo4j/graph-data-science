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

import org.assertj.core.api.Assertions;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.ThrowableRootCauseMatcher.rootCause;
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
            long communityCount = row.getNumber("communityCount").longValue();
            double modularity = row.getNumber("modularity").doubleValue();
            List<Double> modularities = (List<Double>) row.get("modularities");
            long levels = row.getNumber("ranLevels").longValue();
            long preProcessingMillis = row.getNumber("preProcessingMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();

            assertEquals(3, communityCount, "wrong community count");
            assertEquals(2, modularities.size(), "invalud modularities");
            assertEquals(2, levels, "invalid level count");
            assertUserInput(row, "includeIntermediateCommunities", false);
            assertTrue(modularity > 0, "wrong modularity value");
            assertTrue(preProcessingMillis >= 0, "invalid preProcessingTime");
            assertTrue(writeMillis >= 0, "invalid writeTime");
            assertTrue(computeMillis >= 0, "invalid computeTime");
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
            Object maybeList = row.get(writeProperty);
            assertTrue(maybeList instanceof long[]);
            long[] communities = (long[]) maybeList;
            assertEquals(2, communities.length);
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

        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query)
        );

        assertThat(exception, rootCause(
            IllegalArgumentException.class,
            "No value specified for the mandatory configuration parameter `writeProperty`"
        ));
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
                assertEquals(3, row.getNumber("communityCount").longValue(), "wrong community count");
                assertEquals(1, row.getNumber("ranLevels").longValue(), "wrong number of levels");
            }
        );
        assertWriteResult(RESULT, writeProperty);
    }

    // TODO: add a variation of the below with non-defaults
    @Test
    void testCreateConfigWithDefaults() {
        LouvainBaseConfig louvainConfig = LouvainWriteConfig.of(createMinimalConfig(CypherMapWrapper.empty()));
        assertFalse(louvainConfig.includeIntermediateCommunities());
        assertEquals(10, louvainConfig.maxLevels());
        assertEquals(10, louvainConfig.maxIterations());
        assertEquals(0.0001D, louvainConfig.tolerance());
        assertNull(louvainConfig.seedProperty());
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
            Arguments.of(Map.of("minCommunitySize", 1), 3L, new Long[] {11L, 13L, 14L}),
            Arguments.of(Map.of("minCommunitySize", 3), 3L, new Long[] {14L, 13L}),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), 3L, new Long[] {0L, 1L, 2L}),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), 3L, new Long[] {1L, 2L}),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", SEED_PROPERTY), 3L, new Long[] {1L, 2L, 42L}),
            Arguments.of(Map.of("minCommunitySize", 3, "seedProperty", SEED_PROPERTY), 3L, new Long[] {2L, 42L})
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
            assertEquals(expectedCommunityCount, row.getNumber("communityCount"));

        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + WRITE_PROPERTY + ") AS communities ",
            row -> {
                Assertions.assertThat(row.get("communities"))
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
