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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.CommunityHelper.assertCommunities;
import static org.neo4j.gds.GdsCypher.ExecutionModes.STREAM;

class ModularityOptimizationStreamProcTest extends ModularityOptimizationProcTest {

    private static final String GRAPH_NAME = "custom-graph";

    @BeforeEach
    void graphSetup() {
        var createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("weight")
            .withNodeProperty("seed1")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testStreaming() {
        String query = algoBuildStage()
            .streamMode()
            .yields("nodeId", "communityId");

        long[] communities = new long[6];
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("communityId").longValue();
        });

        assertCommunities(communities, UNWEIGHTED_COMMUNITIES);
    }

    @Test
    void testStreamingWeighted() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .yields("nodeId", "communityId");

        long[] communities = new long[6];
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("communityId").longValue();
        });

        assertCommunities(communities, WEIGHTED_COMMUNITIES);
    }

    @Test
    void testStreamingSeeded() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .streamMode()
            .addParameter("seedProperty", "seed1")
            .yields("nodeId", "communityId");

        long[] communities = new long[6];
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            communities[(int) nodeId] = row.getNumber("communityId").longValue();
        });

        assertCommunities(communities, SEEDED_COMMUNITIES);
        assertTrue(communities[0] == 0 && communities[2] == 2);
    }

    @Test
    void testStreamingEstimate() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds", "beta", "modularityOptimization")
            .estimationMode(STREAM)
            .yields();

        runQueryWithRowConsumer(query, (row) -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), Set.of(3L, 4L)),
                Arguments.of(Map.of("minCommunitySize", 3), Set.of(4L)),
                Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), Set.of(0L, 1L)),
                Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), Set.of(0L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteMinCommunitySize(Map<String, Object> parameters, Set<Long> expectedCommunityIds) {
        String query = algoBuildStage()
                .streamMode()
                .addAllParameters(parameters)
                .yields("nodeId", "communityId");

        var actualCommunities = new HashSet<Long>();

        runQueryWithRowConsumer(query, row -> {
            long communityId = row.getNumber("communityId").longValue();
            actualCommunities.add(communityId);
        });

        assertEquals(actualCommunities, expectedCommunityIds);
    }
}
