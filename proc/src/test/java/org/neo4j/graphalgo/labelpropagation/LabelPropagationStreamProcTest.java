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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LabelPropagationStreamProcTest extends LabelPropagationProcTest<LabelPropagationStreamConfig> {
    @Override
    public Class<? extends AlgoBaseProc<?, LabelPropagation, LabelPropagationStreamConfig>> getProcedureClazz() {
        return LabelPropagationStreamProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationProcTest#gdsGraphVariations")
    void testStream(
        GdsCypher.QueryBuilder queryBuilder,
        String desc
    ) {

        String query = queryBuilder
            .algo("gds.labelPropagation")
            .streamMode()
            .yields();

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            actualCommunities.add(id, community);
        });

        assertEquals(actualCommunities, RESULT);
    }

    @Test
    void testStreamWithFilteredNodes() throws Exception {
        db.shutdown();
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(GraphCreateProc.class, LabelPropagationStreamProc.class);

        String queryWithIgnore = "CREATE (c:Ignore {id:12, seed: 0}) " + DB_CYPHER + " CREATE (a)-[:X]->(c), (c)-[:X]->(b)";
        runQuery(queryWithIgnore);

        String graphCreateQuery = GdsCypher
            .call()
            .withNodeLabels("A", "B", "Ignore")
            .withNodeProperty("id")
            .withNodeProperty("seed")
            .withNodeProperty("weight")
            .withAnyRelationshipType()
            .graphCreate("nodeFilteredGraph")
            .yields("nodeCount", "relationshipCount");

        runQueryWithRowConsumer(graphCreateQuery, row -> {
            assertEquals(13L, row.getNumber("nodeCount"));
            assertEquals(12L, row.getNumber("relationshipCount"));
        });

        String query = GdsCypher.call()
            .explicitCreation("nodeFilteredGraph")
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

        assertEquals(actualCommunities, RESULT);
    }

    @Test
    void testEstimate() {
        String query = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .streamEstimation()
            .addAllParameters(createMinimalConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 4))).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1720L,
            "bytesMax", 2232L
        )));
    }

    @Test
    void statsShouldNotHaveWriteProperties() {
        String query = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .statsMode()
            .yields();

        List<String> forbiddenResultColumns = Arrays.asList(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = Collections.singletonList("writeProperty");
        runQueryWithResultConsumer(query, result -> {
            List<String> badResultColumns = result.columns()
                .stream()
                .filter(forbiddenResultColumns::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badResultColumns);
            assertTrue(result.hasNext(), "Result must not be empty.");
            Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
            List<String> badConfigKeys = config.keySet()
                .stream()
                .filter(forbiddenConfigKeys::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badConfigKeys);
        });
    }

    @Override
    public LabelPropagationStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
