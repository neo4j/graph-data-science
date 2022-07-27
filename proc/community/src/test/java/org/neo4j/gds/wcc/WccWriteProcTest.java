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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class WccWriteProcTest extends WccProcTest<WccWriteConfig> {

    private static final String WRITE_PROPERTY = "componentId";
    private static final String SEED_PROPERTY = "seedId";

    @Override
    public Class<? extends AlgoBaseProc<Wcc, DisjointSetStruct, WccWriteConfig, ?>> getProcedureClazz() {
        return WccWriteProc.class;
    }

    @Override
    public WccWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccWriteConfig.of(mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", WRITE_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testWriteYields() {
        loadGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .yields(
                "nodePropertiesWritten",
                "preProcessingMillis",
                "computeMillis",
                "writeMillis",
                "postProcessingMillis",
                "componentCount",
                "componentDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertUserInput(row, "writeProperty", WRITE_PROPERTY);
                assertUserInput(row, "seedProperty", null);

                assertEquals(10L, row.getNumber("nodePropertiesWritten"));

                assertNotEquals(-1L, row.getNumber("preProcessingMillis"));
                assertNotEquals(-1L, row.getNumber("computeMillis"));
                assertNotEquals(-1L, row.getNumber("writeMillis"));
                assertNotEquals(-1L, row.getNumber("postProcessingMillis"));

                assertEquals(3L, row.getNumber("componentCount"));
                assertUserInput(row, "threshold", 0D);
                assertUserInput(row, "consecutiveIds", false);

                assertEquals(MapUtil.map(
                    "p99", 7L,
                    "min", 1L,
                    "max", 7L,
                    "mean", 3.3333333333333335D,
                    "p90", 7L,
                    "p50", 2L,
                    "p999", 7L,
                    "p95", 7L,
                    "p75", 2L
                ), row.get("componentDistribution"));
            }
        );
    }

    @Test
    void testWrite() {
        loadGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3L, row.getNumber("componentCount"));
        });
    }

    @Test
    void testWriteWithNodeLabelFilter() {
        clearDb();
        runQuery("CREATE (nX:Ignore {nodeId: 42}) " + DB_CYPHER + " CREATE (nX)-[:X]->(nA), (nA)-[:X]->(nX), (nX)-[:X]->(nE), (nE)-[:X]->(nX)");

        String graphCreateQuery = GdsCypher
            .call("nodeFilterGraph")
            .graphProject()
            .withNodeLabels("Label", "Label2", "Ignore")
            .withAnyRelationshipType()
            .yields("nodeCount", "relationshipCount");

        runQueryWithRowConsumer(graphCreateQuery, row -> {
            assertEquals(11L, row.getNumber("nodeCount"));
            assertEquals(11L, row.getNumber("relationshipCount"));
        });

        String query = GdsCypher
            .call("nodeFilterGraph")
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addParameter("nodeLabels", Arrays.asList("Label", "Label2"))
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3L, row.getNumber("componentCount"));
        });
    }

    @Test
    void testWriteWithLabel() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(1L, row.getNumber("componentCount"));
        });
    }

    @Test
    void testWriteWithSeed() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("seedId")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addParameter("seedProperty", SEED_PROPERTY)
            .yields("componentCount");

        assertForSeedTests(query, WRITE_PROPERTY);
    }

    @Test
    void testWriteWithSeedAndSameWriteProperty() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withNodeProperty("seedId")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", SEED_PROPERTY)
            .addParameter("seedProperty", SEED_PROPERTY)
            .yields("componentCount");

        assertForSeedTests(query, SEED_PROPERTY);
    }

    @Test
    void testWriteWithSeedOnExplicitGraph() {
        String graphName = "seedGraph";
        String loadQuery = "CALL gds.graph.project(" +
                           "   $graphName, " +
                           "   '*', '*', {nodeProperties: ['seedId']}  " +
                           ")";
        runQuery(loadQuery, MapUtil.map("graphName", graphName));

        String query = GdsCypher
            .call(graphName)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addParameter("seedProperty", SEED_PROPERTY)
            .yields("componentCount");

        assertForSeedTests(query, WRITE_PROPERTY);
    }

    static Stream<Arguments> componentSizeInputs() {
        return Stream.of(
            Arguments.of(Map.of("minComponentSize", 1), new Long[] {0L, 7L, 9L}),
            Arguments.of(Map.of("minComponentSize", 2), new Long[] {0L, 7L}),
            Arguments.of(Map.of("minComponentSize", 1, "consecutiveIds", true), new Long[] {0L, 1L, 2L}),
            Arguments.of(Map.of("minComponentSize", 2, "consecutiveIds", true), new Long[] {0L, 1L}),
            Arguments.of(Map.of("minComponentSize", 1, "seedProperty", SEED_PROPERTY), new Long[] {42L, 46L, 48L}),
            Arguments.of(Map.of("minComponentSize", 2, "seedProperty", SEED_PROPERTY), new Long[] {42L, 46L})
        );
    }

    @ParameterizedTest
    @MethodSource("componentSizeInputs")
    void testWriteWithMinComponentSize(Map<String, Object> parameters, Long[] expectedComponentIds) {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty(SEED_PROPERTY)
            .yields();
        runQuery(createQuery);
        var query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addAllParameters(parameters)
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3L, row.getNumber("componentCount"));
        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + WRITE_PROPERTY + ") AS components ",
            row -> {
                @SuppressWarnings("unchecked") var actualComponents = (List<Long>) row.get("components");
                assertThat(actualComponents, containsInAnyOrder(expectedComponentIds));
            }
        );
    }

    private void assertForSeedTests(String query, String writeProperty) {
        runQueryWithRowConsumer(query, row -> {
            assertEquals(3L, row.getNumber("componentCount"));
        });

        runQueryWithRowConsumer(
            formatWithLocale("MATCH (n) RETURN n.%s AS %s", writeProperty, writeProperty),
            row -> {
                assertTrue(row.getNumber(writeProperty).longValue() >= 42);
            }
        );

        runQueryWithRowConsumer(
            formatWithLocale("MATCH (n) RETURN n.nodeId AS nodeId, n.%s AS %s", writeProperty, writeProperty),
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                final long componentId = row.getNumber(writeProperty).longValue();
                if (nodeId >= 0 && nodeId <= 6) {
                    assertEquals(42, componentId);
                } else {
                    assertTrue(componentId != 42);
                }
            }
        );
    }

    @Test
    void testWriteWithConsecutiveIds() {
        loadGraph(DEFAULT_GRAPH_NAME);
        String query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addParameter("consecutiveIds", true)
            .yields("componentCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(3L, row.getNumber("componentCount"));
        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n." + WRITE_PROPERTY + ") AS components ",
            row -> assertThat((List<Long>) row.get("components"), containsInAnyOrder(0L, 1L, 2L))
        );
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
            .algo("wcc")
            .writeMode()
            .addParameter("writeProperty", "foo")
            .yields("componentCount");

        assertCypherResult(query, List.of(Map.of("componentCount", 0L)));
    }
}
