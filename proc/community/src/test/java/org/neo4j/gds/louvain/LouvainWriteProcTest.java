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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG_ARRAY;
import static org.neo4j.gds.CommunityHelper.assertCommunities;

class LouvainWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {seed: 1})" +        // 0
        ", (b:Node {seed: 1})" +        // 1
        ", (c:Node {seed: 1})" +        // 2
        ", (d:Node {seed: 1})" +        // 3
        ", (e:Node {seed: 1})" +        // 4
        ", (f:Node {seed: 1})" +        // 5
        ", (g:Node {seed: 2})" +        // 6
        ", (h:Node {seed: 2})" +        // 7
        ", (i:Node {seed: 2})" +        // 8
        ", (j:Node {seed: 42})" +       // 9
        ", (k:Node {seed: 42})" +       // 10
        ", (l:Node {seed: 42})" +       // 11
        ", (m:Node {seed: 42})" +       // 12
        ", (n:Node {seed: 42})" +       // 13
        ", (x:Node {seed: 1})" +        // 14

        ", (a)-[:TYPE {weight: 1.0}]->(b)" +
        ", (a)-[:TYPE {weight: 1.0}]->(d)" +
        ", (a)-[:TYPE {weight: 1.0}]->(f)" +
        ", (b)-[:TYPE {weight: 1.0}]->(d)" +
        ", (b)-[:TYPE {weight: 1.0}]->(x)" +
        ", (b)-[:TYPE {weight: 1.0}]->(g)" +
        ", (b)-[:TYPE {weight: 1.0}]->(e)" +
        ", (c)-[:TYPE {weight: 1.0}]->(x)" +
        ", (c)-[:TYPE {weight: 1.0}]->(f)" +
        ", (d)-[:TYPE {weight: 1.0}]->(k)" +
        ", (e)-[:TYPE {weight: 1.0}]->(x)" +
        ", (e)-[:TYPE {weight: 0.01}]->(f)" +
        ", (e)-[:TYPE {weight: 1.0}]->(h)" +
        ", (f)-[:TYPE {weight: 1.0}]->(g)" +
        ", (g)-[:TYPE {weight: 1.0}]->(h)" +
        ", (h)-[:TYPE {weight: 1.0}]->(i)" +
        ", (h)-[:TYPE {weight: 1.0}]->(j)" +
        ", (i)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(k)" +
        ", (j)-[:TYPE {weight: 1.0}]->(m)" +
        ", (j)-[:TYPE {weight: 1.0}]->(n)" +
        ", (k)-[:TYPE {weight: 1.0}]->(m)" +
        ", (k)-[:TYPE {weight: 1.0}]->(l)" +
        ", (l)-[:TYPE {weight: 1.0}]->(n)" +
        ", (m)-[:TYPE {weight: 1.0}]->(n)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LouvainWriteProc.class,
            GraphProjectProc.class
        );
        registerFunctions(AsNodeFunc.class);

        runQuery(
            "CALL gds.graph.project(" +
            "  'myGraph', " +
            "  {Node: {label: 'Node', properties: 'seed'}}, " +
            "  {TYPE: {type: 'TYPE', orientation: 'UNDIRECTED'}}" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testWrite() {

        var query = "CALL gds.louvain.write('myGraph', { writeProperty: 'myFancyCommunity'})" +
                    " YIELD communityCount, modularity, modularities, ranLevels, preProcessingMillis, " +
                    "   computeMillis, writeMillis, postProcessingMillis, communityDistribution, configuration";

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

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) as id, n.myFancyCommunity as community", row -> {
            long communityId = row.getNumber("community").longValue();
            int nodeId = row.getNumber("id").intValue();
            actualCommunities.add(nodeId, communityId);
        });

        assertCommunities(actualCommunities, List.of(
            List.of(0L, 1L, 2L, 3L, 4L, 5L, 14L),
            List.of(6L, 7L, 8L),
            List.of(9L, 10L, 11L, 12L, 13L)
        ));
    }

    @Test
    void testWriteIntermediateCommunities() {

        var query = "CALL gds.louvain.write('myGraph', { writeProperty: 'myFancyCommunity', includeIntermediateCommunities: true})" +
                    " YIELD configuration";

        runQueryWithRowConsumer(query, row -> {
            assertUserInput(row, "includeIntermediateCommunities", true);
        });

        runQueryWithRowConsumer("MATCH (n) RETURN n.myFancyCommunity as myFancyCommunity", row -> {
            assertThat(row.get("myFancyCommunity"))
                .asInstanceOf(LONG_ARRAY)
                .hasSize(2);
        });
    }

    @Test
    void testWriteWithSeeding() {
        var query = "CALL gds.louvain.write('myGraph', { writeProperty: 'myFancyWriteProperty', seedProperty: 'seed'})" +
                    " YIELD communityCount, ranLevels";
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

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) as id, n.myFancyWriteProperty as community", row -> {
            long communityId = row.getNumber("community").longValue();
            int nodeId = row.getNumber("id").intValue();
            actualCommunities.add(nodeId, communityId);
        });

        assertCommunities(actualCommunities, List.of(
            List.of(0L, 1L, 2L, 3L, 4L, 5L, 14L),
            List.of(6L, 7L, 8L),
            List.of(9L, 10L, 11L, 12L, 13L)
        ));
    }


    // FIXME: This doesn't belong here.
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
            Arguments.of(Map.of("minCommunitySize", 1), 3L, List.of(11L, 13L, 14L)),
            Arguments.of(Map.of("minCommunitySize", 3), 3L, List.of(14L, 13L)),
            Arguments.of(Map.of("minCommunitySize", 1, "consecutiveIds", true), 3L, List.of(0L, 1L, 2L)),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), 3L, List.of(0L, 1L)),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", "seed"), 3L, List.of(1L, 2L, 42L)),
            Arguments.of(Map.of("minCommunitySize", 3, "seedProperty", "seed"), 3L, List.of(2L, 42L))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, long expectedCommunityCount, List<Long> expectedCommunityIds) {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withAnyLabel()
            .withAnyRelationshipType()
            .withNodeProperty("seed")
            .yields();
        runQuery(createQuery);

        var query = GdsCypher
            .call(DEFAULT_GRAPH_NAME)
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", "writeProperty")
            .addAllParameters(parameters)
            .yields("communityCount");

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .isEqualTo(expectedCommunityCount);
        });

        runQueryWithRowConsumer(
            "MATCH (n) RETURN collect(DISTINCT n.writeProperty) AS communities ",
            row -> {
                assertThat(row.get("communities"))
                    .asList()
                    .containsExactlyInAnyOrderElementsOf(expectedCommunityIds);
            }
        );
    }
}
