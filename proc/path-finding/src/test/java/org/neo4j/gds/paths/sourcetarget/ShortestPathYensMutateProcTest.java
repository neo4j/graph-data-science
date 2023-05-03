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
package org.neo4j.gds.paths.sourcetarget;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.config.MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY;
import static org.neo4j.gds.config.SourceNodeConfig.SOURCE_NODE_KEY;
import static org.neo4j.gds.config.TargetNodeConfig.TARGET_NODE_KEY;
import static org.neo4j.gds.paths.PathTestUtil.WRITE_RELATIONSHIP_TYPE;

class ShortestPathYensMutateProcTest extends BaseProcTest {
    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
        "  (c:Label)" +
        ", (d:Label)" +
        ", (e:Label)" +
        ", (f:Label)" +
        ", (g:Label)" +
        ", (h:Label)" +
        ", (c)-[:TYPE {cost: 2.0}]->(e)" +
        ", (c)-[:TYPE {cost: 3.0}]->(h)" +
        ", (e)-[:TYPE {cost: 2.0}]->(f)" +
        ", (e)-[:TYPE {cost: 3.0}]->(g)" +
        ", (e)-[:TYPE {cost: 1.0}]->(h)" +
        ", (f)-[:TYPE {cost: 1.0}]->(d)" +
        ", (f)-[:TYPE {cost: 2.0}]->(g)" +
        ", (g)-[:TYPE {cost: 2.0}]->(d)" +
        ", (h)-[:TYPE {cost: 4.0}]->(f)";

    private static final String EXISTING_GRAPH =
        "CREATE" +
            ", (c:Label)" +
            ", (d:Label)" +
            ", (e:Label)" +
            ", (f:Label)" +
            ", (g:Label)" +
            ", (h:Label)" +
            ", (c)-[{w: 3.0}]->(d)" +
            ", (c)-[{w: 2.0}]->(e)" +
            ", (d)-[{w: 4.0}]->(f)" +
            ", (e)-[{w: 1.0}]->(d)" +
            ", (e)-[{w: 2.0}]->(f)" +
            ", (e)-[{w: 3.0}]->(g)" +
            ", (f)-[{w: 2.0}]->(g)" +
            ", (f)-[{w: 1.0}]->(h)" +
            ", (g)-[{w: 2.0}]->(h)";
    private static final String K_KEY = "k";
    long idC;
    long idH;
    long idD;
    long idE;
    long idF;
    long idG;
    long[] ids0;
    long[] ids1;
    long[] ids2;
    double[] costs0;
    double[] costs1;
    double[] costs2;

    String expectedMutatedGraph() {
        return EXISTING_GRAPH +
            // new relationship as a result from mutate
            ", (c)-[:PATH {w: 3.0}]->(h)" +
            ", (c)-[:PATH {w: 3.0}]->(h)" +
            ", (c)-[:PATH {w: 3.0}]->(h)";
    }

    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        mapWrapper = mapWrapper
            .withNumber(SOURCE_NODE_KEY, idC)
            .withNumber(TARGET_NODE_KEY, idD)
            .withNumber(K_KEY, 3);

        if (!mapWrapper.containsKey(MUTATE_RELATIONSHIP_TYPE_KEY)) {
            mapWrapper = mapWrapper.withString(MUTATE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE);
        }

        return mapWrapper;
    }

    @Test
    void testMutate() {
        var config = ShortestPathYensMutateConfig.of(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.yens")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog.get(getUsername(), databaseId(), "graph").graphStore().getUnion();
        var expected = TestSupport.fromGdl(expectedMutatedGraph());

        assertGraphEquals(expected, actual);
    }

    @Test
    void testWeightedMutate() {
        var config = ShortestPathYensMutateConfig.of(createMinimalConfig(CypherMapWrapper.empty()));

        var query = GdsCypher.call("graph")
            .algo("gds.shortestPath.yens")
            .mutateMode()
            .addParameter("sourceNode", config.sourceNode())
            .addParameter("targetNode", config.targetNode())
            .addParameter("k", config.k())
            .addParameter("relationshipWeightProperty", "cost")
            .addParameter("mutateRelationshipType", WRITE_RELATIONSHIP_TYPE)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "relationshipsWritten", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "mutateMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));

        var actual = GraphStoreCatalog.get(getUsername(), databaseId(), "graph").graphStore().getUnion();
        var expected = TestSupport.fromGdl(
            EXISTING_GRAPH +
                // new relationship as a result from mutate
                ", (c)-[:PATH {w: 5.0}]->(h)" +
                ", (c)-[:PATH {w: 7.0}]->(h)" +
                ", (c)-[:PATH {w: 8.0}]->(h)"
        );

        assertGraphEquals(expected, actual);
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            ShortestPathYensMutateProc.class,
            GraphProjectProc.class
        );

        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");
        idG = idFunction.of("g");
        idH = idFunction.of("h");

        ids0 = new long[]{idC, idE, idF, idD};
        ids1 = new long[]{idC, idE, idG, idD};
        ids2 = new long[]{idC, idH, idF, idD};

        costs0 = new double[]{0.0, 2.0, 4.0, 5.0};
        costs1 = new double[]{0.0, 2.0, 5.0, 7.0};
        costs2 = new double[]{0.0, 3.0, 7.0, 8.0};

        runQuery(GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    GraphDatabaseService graphDb() {
        return db;
    }

    DatabaseId databaseId() {
        return DatabaseId.of(graphDb());
    }
}
