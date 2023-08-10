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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;

class WccStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    @Language("Cypher")
    static final String DB_CYPHER = "CREATE" +
        " (nA:Label {nodeId: 0, seedId: 42})" +
        ",(nB:Label {nodeId: 1, seedId: 42})" +
        ",(nC:Label {nodeId: 2, seedId: 42})" +
        ",(nD:Label {nodeId: 3, seedId: 42})" +
        ",(nE:Label2 {nodeId: 4})" +
        ",(nF:Label2 {nodeId: 5})" +
        ",(nG:Label2 {nodeId: 6})" +
        ",(nH:Label2 {nodeId: 7})" +
        ",(nI:Label2 {nodeId: 8})" +
        ",(nJ:Label2 {nodeId: 9})" +
        // {A, B, C, D}
        ",(nA)-[:TYPE]->(nB)" +
        ",(nB)-[:TYPE]->(nC)" +
        ",(nC)-[:TYPE]->(nD)" +
        ",(nD)-[:TYPE {cost:4.2}]->(nE)" + // threshold UF should split here
        // {E, F, G}
        ",(nE)-[:TYPE]->(nF)" +
        ",(nF)-[:TYPE]->(nG)" +
        // {H, I}
        ",(nH)-[:TYPE]->(nI)";

    private long[][] EXPECTED_COMMUNITIES;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            WccStreamProc.class,
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class
        );

        EXPECTED_COMMUNITIES = new long[][]{
            idFunction.of("nA", "nB", "nC", "nD", "nE", "nF", "nG"),
            idFunction.of("nH", "nI"),
            new long[]{
                idFunction.of("nJ")
            }
        };
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testStreamWithDefaults() {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .streamMode()
            .addParameter("minComponentSize", 1)
            .yields("nodeId", "componentId");

        Map<Long, Long> communities = new HashMap<>();
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("componentId").longValue();
            communities.put(nodeId, setId);
        });

        CommunityHelper.assertCommunities(communities, EXPECTED_COMMUNITIES);
    }

    @Test
    void testStreamRunsOnLoadedGraphWithNodeLabelFilter() {
        clearDb();
        runQuery(
            "CREATE (nX:Ignore {nodeId: 42}) " + DB_CYPHER +
                " CREATE (nX)-[:X]->(nA), (nA)-[:X]->(nX), (nX)-[:X]->(nE), (nE)-[:X]->(nX)"
        );

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
            Arguments.of(Map.of("minComponentSize", 1), Map.of(
                "nA", 0L,
                "nB", 0L,
                "nC", 0L,
                "nD", 0L,
                "nE",0L,
                "nF",0L,
                "nG", 0L,
                "nH", 7L,
                "nI", 7L,
                "nJ", 9L
            )),
            Arguments.of(Map.of("minComponentSize", 3), Map.of(
                "nA", 0L,
                "nB", 0L,
                "nC", 0L,
                "nD", 0L,
                "nE",0L,
                "nF",0L,
                "nG", 0L
            ))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamWithMinComponentSize(Map<String, Long> parameters, Map<String, Long> expectedCommunities) {
        var projectQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .loadEverything(Orientation.NATURAL)
            .yields();
        runQuery(projectQuery);
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("wcc")
            .streamMode()
            .addAllParameters(parameters)
            .yields("nodeId", "componentId");

        Map<String, Long> actualComponents = new HashMap<>();
        runQueryWithRowConsumer(query, row -> {
            actualComponents.put(
                idToVariable.of(row.getNumber("nodeId").longValue()),
                row.getNumber("componentId").longValue()
            );
        });

        assertThat(actualComponents).isEqualTo(expectedCommunities);
    }

}
