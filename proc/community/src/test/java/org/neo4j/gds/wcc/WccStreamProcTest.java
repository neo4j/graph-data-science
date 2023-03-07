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
package org.neo4j.gds.wcc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class WccStreamProcTest extends WccProcTest<WccStreamConfig> {

    private static final long[][] EXPECTED_COMMUNITIES = {new long[]{0L, 1L, 2L, 3L, 4, 5, 6}, new long[]{7, 8}, new long[]{9}};

    @Override
    public Class<? extends AlgoBaseProc<Wcc, DisjointSetStruct, WccStreamConfig, ?>> getProcedureClazz() {
        return WccStreamProc.class;
    }

    @Override
    public WccStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccStreamConfig.of(mapWrapper);
    }

    @AfterEach
    void cleanCatalog() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStreamWithDefaults() {
        loadGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .streamMode()
                .addParameter("minComponentSize", 1)
            .yields("nodeId", "componentId");

        long [] communities = new long[10];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        CommunityHelper.assertCommunities(communities, EXPECTED_COMMUNITIES);
    }

    @Test
    void testStreamRunsOnLoadedGraph() {
        GraphProjectConfig graphProjectConfig = ImmutableGraphProjectFromStoreConfig
            .builder()
            .graphName("testGraph")
            .nodeProjections(NodeProjections.all())
            .relationshipProjections(RelationshipProjections.ALL)
            .build();

        GraphStoreCatalog.set(
            graphProjectConfig,
            graphLoader(graphProjectConfig).graphStore()
        );

        String query = GdsCypher.call("testGraph")
            .algo("wcc")
            .streamMode()
            .yields("nodeId", "componentId");

        long [] communities = new long[10];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        CommunityHelper.assertCommunities(communities, EXPECTED_COMMUNITIES);
    }

    @Test
    void testStreamRunsOnLoadedGraphWithNodeLabelFilter() {
        clearDb();
        runQuery("CREATE (nX:Ignore {nodeId: 42}) " + DB_CYPHER + " CREATE (nX)-[:X]->(nA), (nA)-[:X]->(nX), (nX)-[:X]->(nE), (nE)-[:X]->(nX)");

        String graphCreateQuery = GdsCypher
            .call("nodeFilterGraph")
            .graphProject()
            .withNodeLabels("Label", "Label2", "Ignore")
            .withAnyRelationshipType()
            .yields();

        runQueryWithRowConsumer(graphCreateQuery, row -> {
            assertThat(row.getNumber("nodeCount"))
                .asInstanceOf(LONG)
                .isEqualTo(11L);
            assertThat(row.getNumber("relationshipCount"))
                .asInstanceOf(LONG)
                .isEqualTo(11L);
        });

        String query = GdsCypher.call("nodeFilterGraph")
            .algo("wcc")
            .streamMode()
            .addParameter("nodeLabels", Arrays.asList("Label", "Label2"))
            .yields("nodeId", "componentId");

        var actualCommunities = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            actualCommunities.add(row.getNumber("componentId").longValue());
        });

        assertThat(actualCommunities).hasSize(3);
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minComponentSize", 1), new Long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L, 7L, 7L, 9L}),
                Arguments.of(Map.of("minComponentSize", 3), new Long[]{0L, 0L, 0L, 0L, 0L, 0L, 0L})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamWithMinComponentSize(Map<String, Long> parameters, Long[] expectedCommunities) {
        loadGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
                .algo("wcc")
                .streamMode()
                .addAllParameters(parameters)
                .yields("nodeId", "componentId");

        Long [] communities = new Long[expectedCommunities.length];
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities[nodeId] = setId;
        });

        assertThat(communities)
            .containsExactly(expectedCommunities);
    }
}
