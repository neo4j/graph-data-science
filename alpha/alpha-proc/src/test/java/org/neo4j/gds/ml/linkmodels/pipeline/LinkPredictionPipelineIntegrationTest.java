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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPredictionPipelineMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionPipelineTrainProc;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LinkPredictionPipelineIntegrationTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";

    private static final String NODES =
        "CREATE " +
        "(a:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(b:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(c:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(d:N {noise: 42, z: 0, array: [1.0,2.0,3.0,4.0,5.0]}), " +
        "(e:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(f:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(g:N {noise: 42, z: 100, array: [-1.0,2.0,3.0,4.0,5.0]}), " +
        "(h:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(i:N {noise: 42, z: 200, array: [-1.0,-2.0,3.0,4.0,5.0]}), " +
        "(j:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(k:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(l:N {noise: 42, z: 300, array: [-1.0,2.0,3.0,-4.0,5.0]}), " +
        "(m:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(n:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(o:N {noise: 42, z: 400, array: [1.0,2.0,-3.0,4.0,-5.0]}), " +
        "(p:Ignore {noise: -1, z: -1, array: [1.0]}), ";

    @Neo4jGraph
    static String GRAPH =
        NODES +
        "(a)-[:REL]->(b), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL]->(d), " +
        "(b)-[:REL]->(c), " +
        "(b)-[:REL]->(d), " +
        "(c)-[:REL]->(d), " +
        "(e)-[:REL]->(f), " +
        "(e)-[:REL]->(g), " +
        "(f)-[:REL]->(g), " +
        "(h)-[:REL]->(i), " +
        "(j)-[:REL]->(k), " +
        "(j)-[:REL]->(l), " +
        "(k)-[:REL]->(l), " +
        "(m)-[:REL]->(n), " +
        "(m)-[:REL]->(o), " +
        "(n)-[:REL]->(o), " +
        "(a)-[:REL]->(p), " +

        "(a)-[:IGNORED]->(e), " +
        "(m)-[:IGNORED]->(a), " +
        "(m)-[:IGNORED]->(b), " +
        "(m)-[:IGNORED]->(c) ";

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            LinkPredictionPipelineMutateProc.class,
            LinkPredictionPipelineTrainProc.class,
            LinkPredictionPipelineCreateProc.class,
            LinkPredictionPipelineAddStepProcs.class,
            LinkPredictionPipelineConfigureParamsProc.class,
            LinkPredictionPipelineConfigureSplitProc.class
        );

        String createQuery = GdsCypher.call()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog
            .get(getUsername(), db.databaseId(), "g")
            .graphStore();
    }

    @Test
    void trainAndPredict() {
        var topN = 4;

        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        var modelName = "trainedModel";

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: $modelName, negativeClassWeight: 1.0, randomSeed: 1337 }" +
            ")" +
            " YIELD modelInfo" +
            " RETURN modelInfo.modelType AS modelType",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName),
            List.of(Map.of("modelType", "Link prediction pipeline"))
        );

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.predict.mutate($graphName, {" +
            " modelName: $modelName," +
            " mutateRelationshipType: 'PREDICTED'," +
            " threshold: 0," +
            " topN: $topN," +
            " concurrency: $concurrency" +
            "})",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName,"topN", topN, "concurrency", 4),
            List.of(Map.of(
                "createMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", 0L,
                // we are writing undirected rels so we get 2x topN
                "relationshipsWritten", 2L * topN,
                "configuration", isA(Map.class)
            ))
        );

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED"), "probability"));
    }
}
