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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@ExtendWith(SoftAssertionsExtension.class)
class LabelPropagationStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {id: 0, seed: 42}) " +
        ", (b:B {id: 1, seed: 42}) " +

        ", (a)-[:X]->(:A {id: 2,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 3,  weight: 2.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 4,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 5,  weight: 1.0, seed: 1}) " +
        ", (a)-[:X]->(:A {id: 6,  weight: 8.0, seed: 2}) " +

        ", (b)-[:X]->(:B {id: 7,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 8,  weight: 2.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 9,  weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 10, weight: 1.0, seed: 1}) " +
        ", (b)-[:X]->(:B {id: 11, weight: 8.0, seed: 2})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            LabelPropagationStreamProc.class,
            GraphProjectProc.class
        );
        // Create explicit graphs with both projection variants
        runQuery(
            "CALL gds.graph.project(" +
            "   'myGraph', " +
            "   {" +
            "       A: {label: 'A', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}, " +
            "       B: {label: 'B', properties: {seed: {property: 'seed'}, weight: {property: 'weight'}}}" +
            "   }, " +
            "   '*'" +
            ")"
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStream(SoftAssertions assertions) {

        String query = GdsCypher.call("myGraph")
            .algo("gds.labelPropagation")
            .streamMode()
            .yields();

        var expectedCommunities = List.of(2L, 7L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        runQueryWithRowConsumer(query, row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long communityId = row.getNumber("communityId").longValue();

            assertions.assertThat(communityId).isEqualTo(expectedCommunities.get(nodeId));
        });
    }

    // FIXME: This doesn't belong here.
    @Test
    void testEstimate() {
        var query = "CALL gds.labelPropagation.stream.estimate('myGraph', {concurrency: 4})" +
                    " YIELD bytesMin, bytesMax, nodeCount, relationshipCount";

        assertCypherResult(query, List.of(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1640L,
            "bytesMax", 2152L
        )));
    }

    // FIXME: this looks dodgy and unreadable...
    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), new Long[]{2L, 7L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L}),
                Arguments.of(Map.of("minCommunitySize", 2), new Long[]{2L, 7L, 2L, null, null, null, null, 7L, null, null, null, null})
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamMinCommunitySize(Map<String, Long> parameters, Long[] expectedCommunityIds) {
        String query = GdsCypher.call("myGraph")
                .algo("gds.labelPropagation")
                .streamMode()
                .addAllParameters(parameters)
                .yields();

        Long[] actualCommunities = new Long[12];
        runQueryWithRowConsumer(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            actualCommunities[id] = community;
        });

        assertArrayEquals(actualCommunities, expectedCommunityIds);
    }

    @Nested
    class FilteredGraph extends BaseTest {

        @Neo4jGraph(offsetIds = true)
        static final String DB_CYPHER_WITH_OFFSET = DB_CYPHER;

        @Test
        void testStreamWithFilteredNodes() {
            String query = GdsCypher.call("myGraph")
                .algo("gds.labelPropagation")
                .streamMode()
                .addParameter("nodeLabels", Arrays.asList("A", "B"))
                .yields();

            // offset is `42`
            var expectedCommunities = List.of(44L, 49L, 44L, 45L, 46L, 47L, 48L, 49L, 50L, 51L, 52L, 53L);
            var actualCommunities = new ArrayList<Long>();
            runQueryWithRowConsumer(query, row -> {
                long communityId = row.getNumber("communityId").longValue();
                actualCommunities.add(communityId);
            });

            assertThat(actualCommunities).containsExactlyInAnyOrderElementsOf(expectedCommunities);
        }
    }
}
