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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LabelPropagationStreamProcTest extends LabelPropagationProcTest<LabelPropagationStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<LabelPropagation, LabelPropagationResult, LabelPropagationStreamConfig, ?>> getProcedureClazz() {
        return LabelPropagationStreamProc.class;
    }

    @Test
    void testStream(
    ) {

        String query = GdsCypher.call(TEST_GRAPH_NAME)
            .algo("gds.labelPropagation")
            .streamMode()
            .yields();

        Long[] actualCommunities = new Long[RESULT.size()];
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            actualCommunities[id] = community;
        });

        assertEquals(RESULT, Arrays.asList(actualCommunities));
    }

    @Test
    void testEstimate() {
        String query = GdsCypher
            .call(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .streamEstimation()
            .addAllParameters(createMinimalConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 4))).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1640L,
            "bytesMax", 2152L
        )));
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), RESULT.toArray(new Long[0])),
                Arguments.of(Map.of("minCommunitySize", 2), new Long[]{2L, 7L, 2L, null, null, null, null, 7L, null, null, null, null})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamMinCommunitySize(Map<String, Long> parameters, Long[] expectedCommunityIds) {
        String query = GdsCypher.call(TEST_GRAPH_NAME)
                .algo("gds.labelPropagation")
                .streamMode()
                .addAllParameters(parameters)
                .yields();

        Long[] actualCommunities = new Long[RESULT.size()];
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            actualCommunities[id] = community;
        });

        assertArrayEquals(actualCommunities, expectedCommunityIds);
    }

    @Nested
    class FilteredGraph extends BaseTest {

        @Neo4jGraph
        static final String DB_CYPHER_WITH_OFFSET = "CREATE (c:Ignore {id:12, seed: 0}) " + DB_CYPHER + " CREATE (a)-[:X]->(c), (c)-[:X]->(b)";

        @Test
        void testStreamWithFilteredNodes() {

            String graphCreateQuery = GdsCypher
                .call("nodeFilteredGraph")
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

            String query = GdsCypher.call("nodeFilteredGraph")
                .algo("gds.labelPropagation")
                .streamMode()
                .addParameter("nodeLabels", Arrays.asList("A", "B"))
                .yields();

            List<Long> actualCommunities = new ArrayList<>();
            runQueryWithRowConsumer(query, row -> {
                int id = row.getNumber("nodeId").intValue();
                long community = row.getNumber("communityId").longValue();
                actualCommunities.add(id - 1, community - 1);
            });

            assertEquals(RESULT, actualCommunities);
        }
    }

    @Override
    public LabelPropagationStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationStreamConfig.of(mapWrapper);
    }
}
