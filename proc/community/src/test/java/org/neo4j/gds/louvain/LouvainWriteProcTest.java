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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.ConsecutiveIdsConfigTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.config.ConcurrencyConfigProcTest;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.ThrowableRootCauseMatcher.rootCause;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class LouvainWriteProcTest extends LouvainProcTest<LouvainWriteConfig> implements
    ConsecutiveIdsConfigTest<Louvain, LouvainWriteConfig, Louvain> {

    @Override
    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig()),
            ConcurrencyConfigProcTest.writeTest(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<Louvain, Louvain, LouvainWriteConfig>> getProcedureClazz() {
        return LouvainWriteProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.louvain.LouvainProcTest#graphVariations")
    void testWrite(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        String writeProperty = "myFancyCommunity";
        @Language("Cypher") String query = queryBuilder
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .yields(
                "communityCount",
                "modularity",
                "modularities",
                "ranLevels",
                "createMillis",
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
            long createMillis = row.getNumber("createMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long writeMillis = row.getNumber("writeMillis").longValue();

            assertEquals(3, communityCount, "wrong community count");
            assertEquals(2, modularities.size(), "invalud modularities");
            assertEquals(2, levels, "invalid level count");
            assertUserInput(row, "includeIntermediateCommunities", false);
            assertTrue(modularity > 0, "wrong modularity value");
            assertTrue(createMillis >= 0, "invalid loadTime");
            assertTrue(writeMillis >= 0, "invalid writeTime");
            assertTrue(computeMillis >= 0, "invalid computeTime");
        });
        assertWriteResult(RESULT, writeProperty);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.louvain.LouvainProcTest#graphVariations")
    void testWriteIntermediateCommunities(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        String writeProperty = "myFancyCommunity";
        String query = queryBuilder
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
        "writeProperty: null,",
        "writeProperty: '',",
    })
    void testWriteRequiresWritePropertyToBeSet(String writePropertyParameter) {
        String query = "CALL gds.louvain.write({" +
                       writePropertyParameter +
                       "    nodeProjection: ['Node']," +
                       "    relationshipProjection: {" +
                       "      TYPE: {" +
                       "        type: 'TYPE'," +
                       "        orientation: 'UNDIRECTED'" +
                       "      }" +
                       "    }" +
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

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.louvain.LouvainProcTest#graphVariations")
    void testWriteWithSeeding(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        String writeProperty = "myFancyWriteProperty";
        String query = queryBuilder
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
        LouvainBaseConfig louvainConfig = LouvainWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            createMinimalConfig(CypherMapWrapper.empty())
        );
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
        String query = GdsCypher
            .call()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minCommunitySize", 1), new Long[] {8L, 11L, 13L, 14L}),
            Arguments.of(Map.of("minCommunitySize", 3), new Long[] {8L, 13L}),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), new Long[] {0L, 1L, 2L, 3L}),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), new Long[] {2L, 3L}),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", SEED_PROPERTY), new Long[] {1L, 2L, 42L}),
            Arguments.of(Map.of("minCommunitySize", 3, "seedProperty", SEED_PROPERTY), new Long[] {2L, 42L})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, Long[] expectedCommunityIds) {
        var query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty(SEED_PROPERTY)
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addAllParameters(parameters)
            .yields("communityCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(parameters.containsKey("seedProperty") ? 3L : 4L, row.getNumber("communityCount"));

        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + WRITE_PROPERTY + ") AS communities ",
            row -> {
                @SuppressWarnings("unchecked") var actualComponents = (List<Long>) row.get("communities");
                assertThat(actualComponents, containsInAnyOrder(expectedCommunityIds));
            }
        );
    }

    @Override
    public LouvainWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
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
