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
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG_ARRAY;
import static org.assertj.core.api.InstanceOfAssertFactories.SET;

class LouvainWriteProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (b0:Node {seed: 111})" +
        ", (b1:Node {seed: 111})" +
        ", (a0:Node {seed: 222})" +
        ", (a1:Node {seed: 222})" +
        ", (a2:Node {seed: 222})" +
        ", (b0)-[:TYPE]->(b1)"+
        ", (a0)-[:TYPE]->(a1)" +
        ", (a0)-[:TYPE]->(a2)" +
        ", (a1)-[:TYPE]->(a2)";


    @Inject
    private IdFunction idFunction;

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
                    " YIELD communityCount, modularity, modularities, ranLevels, preProcessingMillis, nodePropertiesWritten," +
                    "   computeMillis, writeMillis, postProcessingMillis, communityDistribution, configuration";

        runQueryWithRowConsumer(query, row -> {

            assertThat(row.getNumber("nodePropertiesWritten"))
                .asInstanceOf(LONG)
                .as("wrong node props written ")
                .isEqualTo(5L);

            assertThat(row.getNumber("communityCount"))
                .asInstanceOf(LONG)
                .as("wrong community count")
                .isEqualTo(2L);

            assertThat(row.get("modularities"))
                .asInstanceOf(LIST)
                .as("invalid modularities")
                .hasSize(1);

            assertThat(row.getNumber("ranLevels"))
                .asInstanceOf(LONG)
                .as("invalid level count")
                .isEqualTo(1L);

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

        Map<Long, Long> actualCommunities = new HashMap<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) as id, n.myFancyCommunity as community", row -> {
            long nodeId = row.getNumber("id").longValue();
            actualCommunities.put(nodeId, row.getNumber("community").longValue());
        });
        CommunityHelper.assertCommunities(
            actualCommunities,
            idFunction.of("a0","a1","a2"),
            idFunction.of("b0","b1")
        );
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
                .hasSize(1);
        });
    }

    @Test
    void testWriteWithSeeding() {
        var query = "CALL gds.louvain.write('myGraph', { writeProperty: 'myFancyWriteProperty', seedProperty: 'seed'})" +
                    " YIELD communityCount, ranLevels";
        runQuery(query);

        var actualCommunities = new HashSet<>();
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) as id, n.myFancyWriteProperty as community", row -> {
            actualCommunities.add(row.getNumber("community").longValue());
        });


        assertThat(actualCommunities).containsExactlyInAnyOrderElementsOf(List.of(111L,222L));
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
            Arguments.of(Map.of("minCommunitySize", 3),  1, Optional.empty()),
            Arguments.of(Map.of("minCommunitySize", 3, "consecutiveIds", true), 1, Optional.of(List.of(0L))),
            Arguments.of(Map.of("minCommunitySize", 1, "seedProperty", "seed"), 2,Optional.of(List.of(111L,222L))),
            Arguments.of(Map.of("minCommunitySize", 3, "seedProperty", "seed"), 1,Optional.of(List.of(222L)))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Object> parameters, int expectedSize,Optional<List<Long>> expectedCommunityIds) {

        var query = GdsCypher
            .call("myGraph")
            .algo("louvain")
            .writeMode()
            .addParameter("writeProperty", "writeProperty")
            .addParameter("concurrency", 1)
            .addAllParameters(parameters)
            .yields();

        runQuery(query);

        var hashSet=new HashSet<Long>();
        runQueryWithRowConsumer(
            "MATCH (n)  WHERE  n.writeProperty IS NOT NULL  RETURN n.writeProperty AS community ",
            row -> {
                hashSet.add(row.getNumber("community").longValue());
            }
        );
        assertThat(hashSet).satisfies(
             set-> {
                assertThat(set).hasSize(expectedSize);
                 expectedCommunityIds
                     .ifPresent(exactIds -> assertThat(set).asInstanceOf(SET).containsAll(exactIds));
             });
    }
}
