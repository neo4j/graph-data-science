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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.CommunityHelper.assertCommunities;

class LouvainStreamProcTest extends LouvainProcTest<LouvainStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Louvain, LouvainResult, LouvainStreamConfig, ?>> getProcedureClazz() {
        return LouvainStreamProc.class;
    }

    @Test
    void testStream() {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .streamMode()
            .yields("nodeId", "communityId", "intermediateCommunityIds");

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            assertNull(row.get("intermediateCommunityIds"));
            actualCommunities.add(id, community);
        });
        assertCommunities(actualCommunities, RESULT);
    }

    @Test
    void testStreamConsecutiveIds() {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .streamMode()
            .addParameter("consecutiveIds", true)
            .yields("nodeId", "communityId", "intermediateCommunityIds");

        var communityMap = new HashSet<Long>();

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            communityMap.add(community);
            assertNull(row.get("intermediateCommunityIds"));
            actualCommunities.add(id, community);
        });
        assertCommunities(actualCommunities, RESULT);
        assertThat(communityMap).hasSize(3).containsExactlyInAnyOrder(0L, 1L, 2L);
    }

    @Test
    void testStreamCommunities() {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
            .algo("louvain")
            .streamMode()
            .addParameter("includeIntermediateCommunities", true)
            .yields("nodeId", "communityId", "intermediateCommunityIds");

        runQueryWithRowConsumer(query, row -> {
            Object maybeList = row.get("intermediateCommunityIds");
            assertTrue(maybeList instanceof List);
            List<Long> communities = (List<Long>) maybeList;
            assertEquals(2, communities.size());
            assertEquals(communities.get(1), row.getNumber("communityId").longValue());
        });
    }

    @Test
    void testCreateConfigWithDefaults() {
        LouvainBaseConfig louvainConfig = LouvainStreamConfig.of(CypherMapWrapper.empty());
        assertEquals(false, louvainConfig.includeIntermediateCommunities());
        assertEquals(10, louvainConfig.maxLevels());
    }

    @Test
    void testStreamNodeFiltered() {
        var graphName = "multi_label";

        runQuery("CREATE (:Ignored)");

        long totalNodeCount = runQuery(
            "MATCH (n) RETURN count(n) AS count",
            result -> (Long) result.next().get("count")
        );
        long filteredNodeCount = runQuery(
            "MATCH (n:Node) RETURN count(n) AS count",
            result -> (Long) result.next().get("count")
        );

        assertThat(totalNodeCount).isGreaterThan(filteredNodeCount);

        // project graph including the to-be-ignored label
        runQuery(GdsCypher.call(graphName)
            .graphProject()
            .withNodeLabel("Node")
            .withNodeLabel("Ignored")
            .withAnyRelationshipType()
            .yields());

        @Language("Cypher") String query = GdsCypher.call(graphName)
            .algo("louvain")
            .streamMode()
            // execute Louvain on node subset
            .addParameter("nodeLabels", List.of("Node"))
            .yields("nodeId");

        var actualFilteredCount = runQuery(
            query + " RETURN count(nodeId) AS count",
            result -> (Long) result.next().get("count")
        );

        assertThat(actualFilteredCount).isEqualTo(filteredNodeCount);
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), List.of(0, 1, 2)),
                Arguments.of(Map.of("minCommunitySize", 5), List.of(0, 2))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testMinCommunitySize(Map<String, Long> parameters, List<Integer> expectedCommunityIds) {
        @Language("Cypher") String query = GdsCypher.call("myGraph")
                .algo("louvain")
                .streamMode()
                .addParameter("consecutiveIds", true)
                .addAllParameters(parameters)
                .yields("nodeId", "communityId");

        var communityMap = new HashSet<Integer>();

        runQueryWithRowConsumer(query, row -> {
            long id = row.getNumber("nodeId").longValue();
            int community = row.getNumber("communityId").intValue();

            communityMap.add(community);
            assertThat(RESULT.get(community)).contains(id);
        });
        assertThat(communityMap).containsExactlyElementsOf(expectedCommunityIds);
    }

    @Override
    public LouvainStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainStreamConfig.of(mapWrapper);
    }
}
