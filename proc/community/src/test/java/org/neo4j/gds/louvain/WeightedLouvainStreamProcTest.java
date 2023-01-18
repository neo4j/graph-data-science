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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.Result;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.compat.MapUtil.map;

class WeightedLouvainStreamProcTest extends LouvainProcTest<LouvainStreamConfig> {

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

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
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

    @Override
    List<String> graphProjectQueries() {
        return Arrays.asList(
            GdsCypher.call("weightedLouvainGraph")
                .graphProject()
                .withNodeLabel("User")
                .withNodeProperty("seed")
                .withRelationshipType(
                    "LINK",
                    RelationshipProjection.of(
                        "LINK",
                        Orientation.UNDIRECTED,
                        Aggregation.NONE
                    )
                )
                .withRelationshipProperty(PropertyMapping.of("weight", 0.0d))
                .yields(),
            GdsCypher.call("unweightedGraph")
                .graphProject()
                .withNodeLabel("User")
                .withNodeProperty("seed")
                .withRelationshipType(
                    "LINK",
                    RelationshipProjection.of(
                        "LINK",
                        Orientation.UNDIRECTED,
                        Aggregation.NONE
                    )
                )
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

    @Test
    void weightedLouvainTest() {
        var query = GdsCypher
            .call("weightedLouvainGraph")
            .algo("louvain")
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .yields("nodeId", "communityId", "intermediateCommunityIds")
            .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, intermediateCommunityIds")
            .concat(" ORDER BY name ASC");
        QueryRunner.runQueryWithRowConsumer(db, query, row -> assertLouvainResultRow(row, expectedWeightedResult));
    }

    static Stream<Arguments> unweightedGraphQueries() {
        return Stream.of(
            arguments(
                GdsCypher
                    .call("unweightedGraph")
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "intermediateCommunityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, intermediateCommunityIds")
                    .concat(" ORDER BY name ASC"),
                "explicit graph created without weights"
            ),
            arguments(
                GdsCypher
                    .call("weightedLouvainGraph")
                    .algo("louvain")
                    .streamMode()
                    .yields("nodeId", "communityId", "intermediateCommunityIds")
                    .concat(" RETURN gds.util.asNode(nodeId).name as name, communityId, intermediateCommunityIds")
                    .concat(" ORDER BY name ASC"),
                "explicit graph created with weights"
            )
        );
    }

    private void assertLouvainResultRow(Result.ResultRow row, Map<String, Object> expectedResult) {
        String computedName = row.getString("name");
        Object computedCommunityId = row.get("communityId");

        assertEquals(expectedResult.get(computedName), computedCommunityId);
        assertNull(row.get("intermediateCommunityIds"));
    }

    @Override
    public Class<? extends AlgoBaseProc<Louvain, LouvainResult, LouvainStreamConfig, ?>> getProcedureClazz() {
        return LouvainStreamProc.class;
    }

    @Override
    public LouvainStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainStreamConfig.of(mapWrapper);
    }
}
