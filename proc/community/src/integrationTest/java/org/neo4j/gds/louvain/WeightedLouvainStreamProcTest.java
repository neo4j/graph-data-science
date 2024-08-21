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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.AsNodeFunc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WeightedLouvainStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
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

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            LouvainStreamProc.class,
            GraphProjectProc.class
        );

        registerFunctions(AsNodeFunc.class);

        runQuery(GdsCypher.call("weightedLouvainGraph")
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
            .yields());
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

        Map<String, Object> expectedResult = Map.of(
            "Alice", 3L,
            "Bridget", 2L,
            "Charles", 2L,
            "Doug", 3L,
            "Mark", 4L,
            "Michael", 4L
        );

        var rowCount = QueryRunner.runQueryWithRowConsumer(db, query, row -> {
            var name = row.getString("name");
            var communityId = row.get("communityId");

            assertThat(expectedResult).containsEntry(name, communityId);

            assertThat(row.get("intermediateCommunityIds")).isNull();
        });

        assertThat(rowCount).isEqualTo(expectedResult.size());
    }
}
