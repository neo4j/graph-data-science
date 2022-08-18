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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.QueryRunner;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.embeddings.graphsage.GraphSageMutateProc;
import org.neo4j.gds.embeddings.graphsage.GraphSageTrainProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPredictionPipelineMutateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.predict.LinkPredictionPipelineStreamProc;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionPipelineTrainProc;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.model.catalog.ModelListProc;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Neo4jModelCatalogExtension
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
        "(a)-[:REL {weight: 5.0}]->(b), " +
        "(a)-[:REL {weight: 2.0}]->(c), " +
        "(a)-[:REL {weight: 4.0}]->(d), " +
        "(b)-[:REL {weight: 5.0}]->(c), " +
        "(b)-[:REL {weight: 3.0}]->(d), " +
        "(c)-[:REL {weight: 2.0}]->(d), " +
        "(e)-[:REL {weight: 1.0}]->(f), " +
        "(e)-[:REL {weight: 2.0}]->(g), " +
        "(f)-[:REL {weight: 5.0}]->(g), " +
        "(h)-[:REL {weight: 5.0}]->(i), " +
        "(j)-[:REL {weight: 5.0}]->(k), " +
        "(j)-[:REL {weight: 4.0}]->(l), " +
        "(k)-[:REL {weight: 5.0}]->(l), " +
        "(m)-[:REL {weight: 4.0}]->(n), " +
        "(m)-[:REL {weight: 5.0}]->(o), " +
        "(n)-[:REL {weight: 2.0}]->(o), " +
        "(a)-[:REL {weight: 5.0}]->(p), " +

        "(a)-[:IGNORED]->(e), " +
        "(m)-[:IGNORED]->(a), " +
        "(m)-[:IGNORED]->(b), " +
        "(m)-[:IGNORED]->(c) ";

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphSageTrainProc.class,
            GraphSageMutateProc.class,
            ModelListProc.class,
            LinkPredictionPipelineMutateProc.class,
            LinkPredictionPipelineTrainProc.class,
            LinkPredictionPipelineCreateProc.class,
            LinkPredictionPipelineAddStepProcs.class,
            LinkPredictionPipelineAddTrainerMethodProcs.class,
            LinkPredictionPipelineConfigureSplitProc.class,
            LinkPredictionPipelineStreamProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog
            .get("", db.databaseId(), "g")
            .graphStore();
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainAndPredictLR() {
        var topN = 4;

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe', {validationFolds: 2})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe', {penalty: 1})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe', {penalty: 2})");

        var modelName = "trainedModel";

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: $modelName, negativeClassWeight: 1.0, randomSeed: 1337 }" +
            ")" +
            " YIELD modelInfo" +
            " RETURN modelInfo.modelType AS modelType",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName),
            List.of(Map.of("modelType", "LinkPrediction"))
        );

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.mutate($graphName, {" +
            " modelName: $modelName," +
            " mutateRelationshipType: 'PREDICTED'," +
            " threshold: 0," +
            " topN: $topN," +
            " concurrency: $concurrency" +
            "})",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName,"topN", topN, "concurrency", 4),
            List.of(Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", 0L,
                // we are writing undirected rels so we get 2x topN
                "relationshipsWritten", 2L * topN,
                "configuration", isA(Map.class),
                "samplingStats", isA(Map.class),
                "probabilityDistribution", isA(Map.class)
            ))
        );

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED"), "probability"));
    }

    @Test
    void trainAndPredictRF() {
        var topN = 4;

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe', {validationFolds: 2})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.pipeline.linkPrediction.addRandomForest('pipe', {" +
                 "numberOfDecisionTrees: 2, maxDepth: 5, minSplitSize: 2" +
                 "})");

        var modelName = "trainedModel";

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: $modelName, negativeClassWeight: 1.0, randomSeed: 1337 }" +
            ")" +
            " YIELD modelInfo" +
            " RETURN modelInfo.modelType AS modelType",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName),
            List.of(Map.of("modelType", "LinkPrediction"))
        );

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.predict.mutate($graphName, {" +
            " modelName: $modelName," +
            " mutateRelationshipType: 'PREDICTED'," +
            " threshold: 0," +
            " topN: $topN," +
            " concurrency: $concurrency" +
            "})",
            Map.of("graphName", GRAPH_NAME, "modelName", modelName,"topN", topN, "concurrency", 4),
            List.of(Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", 0L,
                // we are writing undirected rels so we get 2x topN
                "relationshipsWritten", 2L * topN,
                "configuration", isA(Map.class),
                "samplingStats", isA(Map.class),
                "probabilityDistribution", isA(Map.class)
            ))
        );

        assertTrue(graphStore.hasRelationshipProperty(RelationshipType.of("PREDICTED"), "probability"));
    }

    @Override
    protected String getUsername() {
        return "myUser";
    }

    @Test
    void runWithGraphSage() {
        runQueryWithUser("CALL gds.graph.project('g_2',{ N: { properties: ['z']}}, {REL: {orientation: 'UNDIRECTED', properties: ['weight']}})");

        runQueryWithUser("CALL gds.beta.graphSage.train(" +
                         "  'g_2'," +
                         "  {" +
                         "    modelName: 'exampleTrainModel'," +
                         "    relationshipWeightProperty: 'weight'," +
                         "    featureProperties: ['z']," +
                         "    aggregator: 'mean'," +
                         "    activationFunction: 'sigmoid'," +
                         "    randomSeed: 1337," +
                         "    sampleSizes: [25, 10]" +
                         "  })");

        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.create('myPipe') ");
        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.configureSplit('myPipe', {validationFolds: 2, testFraction: 0.3, trainFraction: 0.3})");
        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('myPipe', 'beta.graphSage', {" +
                 "modelName: 'exampleTrainModel', " +
                 "mutateProperty: 'embedding'}" +
                 ") ");

        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.addFeature('myPipe', 'L2', {nodeProperties: ['embedding']})");

        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('myPipe')");

        runQueryWithUser("CALL gds.beta.pipeline.linkPrediction.train('g_2', {pipeline: 'myPipe', modelName: 'model'})");

        runQueryWithUser(
            "CALL gds.beta.pipeline.linkPrediction.predict.stream('g_2', {" +
            "   modelName: 'model', topN: 2" +
            "})",
            result -> assertThat(result.stream().count()).isEqualTo(2)
        );
    }

    private void runQueryWithUser(String query) {
        QueryRunner.runQuery(db, getUsername(), query, Map.of());
    }

    private void runQueryWithUser(String query, Consumer<Result> resultConsumer) {
        QueryRunner.runQueryWithResultConsumer(db, getUsername(), query, Map.of(), resultConsumer);
    }
}
