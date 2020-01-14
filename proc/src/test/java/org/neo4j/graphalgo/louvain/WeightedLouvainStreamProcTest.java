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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.GraphLoadProc;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.graphalgo.compat.MapUtil.map;

class WeightedLouvainStreamProcTest extends BaseProcTest {

    private static final Map<String, Object> expectedResult = map(
        "Alice", 3L,
        "Bridget", 2L,
        "Charles", 2L,
        "Doug", 3L,
        "Mark", 5L,
        "Michael", 5L
    );

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase(
            dbBuilder -> dbBuilder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
        );

        @Language("Cypher") String cypher =
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
            ", (nMark)-[:LINK {weight: 1}]->(nDoug)" +
            ", (nMark)-[:LINK {weight: 1}]->(nMichael)" +
            ", (nMichael)-[:LINK {weight: 1}]->(nMark)";

        registerProcedures(LouvainStreamProc.class, LouvainWriteProc.class, GraphLoadProc.class, GraphCreateProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(cypher);
        runQuery("CALL gds.graph.create(" +
                 "    'weightedGraph'," +
                 "    {" +
                 "      Node: {" +
                 "        label: 'User'," +
                 "        properties: ['seed']" +
                 "      }" +
                 "    }," +
                 "    {" +
                 "      LINK: {" +
                 "        type: 'LINK'," +
                 "        projection: 'UNDIRECTED'," +
                 "        aggregation: 'NONE'," +
                 "        properties: ['weight']" +
                 "      }" +
                 "    }" +
                 ")");
    }

    @AfterEach
    void cleanup() {
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Test
    void weightedLouvainTest() {
        String query = "CALL gds.louvain.stream('weightedGraph') " +
                       "YIELD nodeId, communityId, communityIds\n" +
                       "RETURN algo.asNode(nodeId).name as name, communityId, communityIds\n" +
                       "ORDER BY name ASC";

        QueryRunner.runQueryWithRowConsumer(db, query, this::assertLouvainResultRow);
    }

    @Test
    void weightedLouvainImplicitGraphTest() {
        String query = "CALL gds.louvain.stream({\n" +
                       "    nodeProjection: ['User'],\n" +
                       "    relationshipProjection: {\n" +
                       "        LINK: {\n" +
                       "            type: 'LINK',\n" +
                       "            projection: 'UNDIRECTED',\n" +
                       "            aggregation: 'NONE',\n" +
                       "            properties: ['weight']\n" +
                       "        }\n" +
                       "    },\n" +
                       "    weightProperty: 'weight'\n" +
                       "}) YIELD nodeId, communityId, communityIds\n" +
                       "RETURN algo.asNode(nodeId).name as name, communityId, communityIds\n" +
                       "ORDER BY name ASC";

        QueryRunner.runQueryWithRowConsumer(db, query, this::assertLouvainResultRow);
    }
    
    private void assertLouvainResultRow(Result.ResultRow row) {
        String computedName = row.getString("name");
        Object computedCommunityId = row.get("communityId");

        assertEquals(expectedResult.get(computedName), computedCommunityId);
        assertNull(row.get("communityIds"));
    }
}
