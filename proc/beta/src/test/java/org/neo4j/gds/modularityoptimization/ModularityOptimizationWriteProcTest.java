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
package org.neo4j.gds.modularityoptimization;

import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.compat.MapUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.GdsCypher.ExecutionModes.WRITE;

class ModularityOptimizationWriteProcTest extends ModularityOptimizationProcTest {

    private static final String COMMUNITY = "community";
    private static final String COMMUNITY_COUNT = "communityCount";

    @Test
    void testWriting() {
        String query = algoBuildStage()
            .writeMode()
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            // this value changed after adapting to OUTGOING
            assertEquals(-0.0408, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber(COMMUNITY_COUNT).longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });

        assertWriteResult(UNWEIGHTED_COMMUNITIES);
    }
    @Test
    void testWritingUndirected() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.12244, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber(COMMUNITY_COUNT).longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });

        assertWriteResult(UNWEIGHTED_COMMUNITIES);
    }

    @Test
    void testWritingWeighted() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("weight")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(true, row.getBoolean("didConverge"));
            assertEquals(0.4985, row.getNumber("modularity").doubleValue(), 0.001);
            assertEquals(2, row.getNumber(COMMUNITY_COUNT).longValue());
            assertTrue(row.getNumber("ranIterations").longValue() <= 3);
        });

        assertWriteResult(WEIGHTED_COMMUNITIES);
    }

    @Test
    void testWritingSeeded() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeProperty("seed1")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("seedProperty", "seed1")
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQuery(query);

        long[] communities = new long[6];
        MutableInt i = new MutableInt(0);
        runQueryWithRowConsumer("MATCH (n) RETURN n.community as community", (row) -> {
            communities[i.getAndIncrement()] = row.getNumber(COMMUNITY).longValue();
        });
        assertCommunities(communities, SEEDED_COMMUNITIES);
    }

    @Test
    void testWritingTolerance() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("tolerance", 1)
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertTrue(row.getBoolean("didConverge"));
            // Cannot converge after one iteration,
            // because it doesn't have anything to compare the computed modularity against.
            assertEquals(2, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void testWritingIterations() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .writeMode()
            .addParameter("maxIterations", 1)
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertFalse(row.getBoolean("didConverge"));
            assertEquals(1, row.getNumber("ranIterations").longValue());
        });
    }

    @Test
    void testWritingEstimate() {
        String query = algoBuildStage()
            .estimationMode(WRITE)
            .addParameter("writeProperty", COMMUNITY)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), List.of(3L, 4L)),
                Arguments.of(Map.of("minCommunitySize", 3), List.of(4L)),
                Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), List.of(0L, 1L)),
                Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), List.of(0L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteMinCommunitySize(Map<String, Object> parameters, List<Long> expectedCommunityIds) {
        Long totalCommunities = 2L;
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
                .graphProject()
                .withAnyLabel()
                .withAnyRelationshipType()
                .yields();
        runQuery(createQuery);

        var query = GdsCypher
                .call(DEFAULT_GRAPH_NAME)
                .algo("gds", "beta", "modularityOptimization")
                .writeMode()
                .addParameter("writeProperty", COMMUNITY)
                .addAllParameters(parameters)
                .yields(COMMUNITY_COUNT);

        runQueryWithRowConsumer(query, row -> {
            assertEquals(totalCommunities, row.getNumber(COMMUNITY_COUNT));
        });

        runQueryWithRowConsumer(
                "MATCH (n) RETURN collect(DISTINCT n." + COMMUNITY + ") AS communities ",
                row -> Assertions.assertThat(row.get("communities"))
                        .asList()
                        .containsExactlyInAnyOrderElementsOf(expectedCommunityIds)
        );
    }

    private void assertWriteResult(long[]... expectedCommunities) {
        Map<String, Object> nameMapping = MapUtil.map(
            "a", 0,
            "b", 1,
            "c", 2,
            "d", 3,
            "e", 4,
            "f", 5
        );
        long[] actualCommunities = new long[6];
        runQueryWithRowConsumer("MATCH (n) RETURN n.name as name, n.community as community", (row) -> {
            long community = row.getNumber(COMMUNITY).longValue();
            String name = row.getString("name");
            actualCommunities[(int) nameMapping.get(name)] = community;
        });

        assertCommunities(actualCommunities, expectedCommunities);
    }
}
