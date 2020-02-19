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

package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphdb.Result;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class WeightedLouvainStreamProcTest extends LouvainBaseProcTest<LouvainStreamConfig> {

    private static final Map<String, Object> expectedWeightedResult = map(
        "Alice", 3L,
        "Bridget", 2L,
        "Charles", 2L,
        "Doug", 3L,
        "Mark", 4L,
        "Michael", 4L
    );

    private static final Map<String, Object> expectedUnweightedResult = map(
        "Alice", 2L,
        "Bridget", 2L,
        "Charles", 2L,
        "Doug", 4L,
        "Mark", 4L,
        "Michael", 4L
    );

    @Override
    String dbCypher() {
        return "CREATE" +
               "  (nAlice:User {name: 'Alice', seed: 42})" +
               ", (nBridget:User {name: 'Bridget', seed: 42})" +
               ", (nCharles:User {name: 'Charles', seed: 42})" +
               ", (nDoug:User {name: 'Doug'})" +
               ", (nMark:User {name: 'Mark'})" +
               ", (nMichael:User {name: 'Michael'})" +
               ", (nAlice)-[:LINK {weight: 1}]->(nBridget)" +
               ", (nAlice)-[:LINK {weight: 1}]->(nCharles)" +
               ", (nCharles)-[:LINK {weight: 1}]->(nBridget)" +
               ", (nAlice)-[:LINK {weight: 5}]->(nDoug)" +
               ", (nAlice)-[:LINK  {weight: null}]->(nMark)" +
               ", (nMark)-[:LINK {weight: 1}]->(nDoug)" +
               ", (nMark)-[:LINK {weight: 1}]->(nMichael)" +
               ", (nMichael)-[:LINK {weight: 1}]->(nMark)";
    }

    @Override
    List<String> graphCreateQueries() {
        return Arrays.asList(
            GdsCypher.call()
                .withNodeLabel("User")
                .withNodeProperty("seed")
                .withRelationshipType(
                    "LINK",
                    RelationshipProjection.of(
                        "LINK",
                        Projection.UNDIRECTED,
                        Aggregation.NONE
                    )
                )
                .withRelationshipProperty(PropertyMapping.of("weight", 0.0d))
                .graphCreate("weightedLouvainGraph")
                .yields(),
            GdsCypher.call()
                .withNodeLabel("User")
                .withNodeProperty("seed")
                .withRelationshipType(
                    "LINK",
                    RelationshipProjection.of(
                        "LINK",
                        Projection.UNDIRECTED,
                        Aggregation.NONE
                    )
                )
                .graphCreate("unweightedGraph")
                .yields()
        );
    }

    @ParameterizedTest(name = " {1}")
    @MethodSource("unweightedGraphQueries")
    void unweightedLouvainTest(String query, String graphCreation) {
        assumeFalse(
            graphCreation.equals("implicit graph created with weights"),
            "Disable `implicit graph created with weights` until cleared up what the expected behaviour should be"
        );

        QueryRunner.runQueryWithRowConsumer(
            db, query, row -> assertLouvainResultRow(row, expectedUnweightedResult));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("weightedGraphQueries")
    void weightedLouvainTest(String query, String graphMode) {
        QueryRunner.runQueryWithRowConsumer(db, query, row -> assertLouvainResultRow(row, expectedWeightedResult));
    }

    static Stream<Arguments> unweightedGraphQueries() {
        return Stream.of(
            arguments(
                GdsCypher
                    .call()
                    .explicitCreation("unweightedGraph")
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "explicit graph created without weights"
            ),
            arguments(
                GdsCypher
                    .call()
                    .explicitCreation("weightedLouvainGraph")
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "explicit graph created with weights"
            ),
            arguments(
                GdsCypher
                    .call()
                    .implicitCreation(
                        ImmutableGraphCreateFromStoreConfig.builder()
                            .graphName("implicitWeightedGraph")
                            .nodeProjections(NodeProjections.fromString("User"))
                            .relationshipProjections(RelationshipProjections.builder()
                                .putProjection(
                                    ElementIdentifier.of("LINK"),
                                    RelationshipProjection.builder()
                                        .type("LINK")
                                        .projection(Projection.UNDIRECTED)
                                        .aggregation(Aggregation.NONE)
                                        .build()
                                ).build()
                            ).build()
                    )
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "implicit graph created without weights"
            ),
            arguments(
                GdsCypher
                    .call()
                    .implicitCreation(
                        ImmutableGraphCreateFromStoreConfig.builder()
                            .graphName("implicitWeightedGraph")
                            .nodeProjections(NodeProjections.fromString("User"))
                            .relationshipProjections(RelationshipProjections.builder()
                                .putProjection(
                                    ElementIdentifier.of("LINK"),
                                    RelationshipProjection.builder()
                                        .type("LINK")
                                        .projection(Projection.UNDIRECTED)
                                        .aggregation(Aggregation.NONE)
                                        .addProperty(PropertyMapping.of("weight", 0.0d))
                                        .build()
                                ).build()
                            ).build()
                    )
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "implicit graph created with weights"
            )
        );
    }

    static Stream<Arguments> weightedGraphQueries() {
        return Stream.of(
            arguments(
                GdsCypher
                    .call()
                    .explicitCreation("weightedLouvainGraph")
                    .algo("louvain")
                    .streamMode()
                    .addParameter("relationshipWeightProperty", "weight")
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "explicit graph"
            ),
            arguments(
                GdsCypher
                    .call()
                    .implicitCreation(
                        ImmutableGraphCreateFromStoreConfig.builder()
                            .graphName("implicitWeightedGraph")
                            .nodeProjections(NodeProjections.fromString("User"))
                            .relationshipProjections(RelationshipProjections.builder()
                                .putProjection(
                                    ElementIdentifier.of("LINK"),
                                    RelationshipProjection.builder()
                                        .type("LINK")
                                        .projection(Projection.UNDIRECTED)
                                        .aggregation(Aggregation.NONE)
                                        .addProperty(PropertyMapping.of("weight", 0.0d))
                                        .build()
                                ).build()
                            ).build()
                    )
                    .algo("louvain")
                    .streamMode()
                    .addParameter("relationshipWeightProperty", "weight")
                    .yields("nodeId", "communityId", "communityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, communityIds")
                    .concat(" ORDER BY name ASC"),
                "implicit graph"
            )
        );
    }

    private void assertLouvainResultRow(Result.ResultRow row, Map<String, Object> expectedResult) {
        String computedName = row.getString("name");
        Object computedCommunityId = row.get("communityId");

        assertEquals(expectedResult.get(computedName), computedCommunityId);
        assertNull(row.get("communityIds"));
    }

    @Override
    public Class<? extends AlgoBaseProc<?, Louvain, LouvainStreamConfig>> getProcedureClazz() {
        return LouvainStreamProc.class;
    }

    @Override
    public LouvainStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
