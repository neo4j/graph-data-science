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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineAddStepProcs;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineConfigureParamsProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineConfigureSplitProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.model.catalog.ModelDropProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;

class LinkPredictionPipelineTrainProcTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";
    // Five cliques of size 2, 3, or 4
    @Neo4jGraph
    static String GRAPH =
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
        "(p:Ignore {noise: -1, z: -1, array: [1.0]}), " +

        "(a)-[:REL { weight: 1.0 } ]->(b), " +
        "(a)-[:REL]->(c), " +
        "(a)-[:REL { weight: 1.0 } ]->(d), " +
        "(b)-[:REL]->(c), " +
        "(b)-[:REL]->(d), " +
        "(c)-[:REL { weight: 1.0 } ]->(d), " +
        "(e)-[:REL { weight: 1.0 } ]->(f), " +
        "(e)-[:REL]->(g), " +
        "(f)-[:REL { weight: 1.0 } ]->(g), " +
        "(h)-[:REL]->(i), " +
        "(j)-[:REL { weight: 1.0 } ]->(k), " +
        "(j)-[:REL]->(l), " +
        "(k)-[:REL { weight: 1.0 } ]->(l), " +
        "(m)-[:REL { weight: 1.0 } ]->(n), " +
        "(m)-[:REL { weight: 1.0 } ]->(o), " +
        "(n)-[:REL { weight: 1.0 } ]->(o), " +
        "(a)-[:REL]->(p), " +

        "(a)-[:IGNORED { weight: 1.0 } ]->(e), " +
        "(m)-[:IGNORED { weight: 1.0 } ]->(a), " +
        "(m)-[:IGNORED { weight: 1.0 } ]->(b), " +
        "(m)-[:IGNORED { weight: 1.0 } ]->(c) ";

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            LinkPredictionPipelineTrainProc.class,
            LinkPredictionPipelineCreateProc.class,
            LinkPredictionPipelineAddStepProcs.class,
            LinkPredictionPipelineConfigureParamsProc.class,
            LinkPredictionPipelineConfigureSplitProc.class,
            GraphCreateProc.class,
            ModelDropProc.class
        );

        String createQuery = GdsCypher.call()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withRelationshipProperty("weight", DefaultValue.of(1.0))
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        String createQueryWeighted = GdsCypher.call()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withRelationshipProperty("weight", DefaultValue.of(100000.0))
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate("weighted_graph")
            .yields();

        runQuery(createQuery);
        runQuery(createQueryWeighted);
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.removeAllLoadedModels();
    }

    @Test
    void trainAModel() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337 }" +
            ")",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of(
                    "modelInfo", Matchers.allOf(
                        Matchers.hasEntry("modelName", "trainedModel"),
                        Matchers.hasEntry("modelType", "Link prediction pipeline"),
                        Matchers.hasKey("bestParameters"),
                        Matchers.hasKey("metrics"),
                        Matchers.hasKey("trainingPipeline")
                    ),
                    "trainMillis", greaterThan(-1L),
                    "configuration", aMapWithSize(9)
                ))
        );

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();

        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }

    @Test
    void failsWhenMissingFeatures() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");

        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
                    "   $graphName, " +
                    "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337 }" +
                    ")",
            Map.of("graphName", GRAPH_NAME),
            "Training a Link prediction pipeline requires at least one feature. You can add features with the procedure `gds.alpha.ml.pipeline.linkPrediction.addFeature`.");
    }

    @Test
    void failOnAnonymousGraph() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");

        assertError("CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
                    "   { nodeProjection: '*', relationshipProjection: '*', pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337 }" +
                    ")",
            "Link Prediction Pipeline cannot be used with anonymous graphs. Please load the graph before"
        );
    }

    @Test
    void trainOnNodeLabelFilteredGraph() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['array', 'pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        assertCypherResult(
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337, nodeLabels: ['N'] }" +
            ")",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of(
                    "modelInfo", Matchers.allOf(
                        Matchers.hasEntry("modelName", "trainedModel"),
                        Matchers.hasEntry("modelType", "Link prediction pipeline"),
                        Matchers.hasKey("bestParameters"),
                        Matchers.hasKey("metrics"),
                        Matchers.hasKey("trainingPipeline")
                    ),
                    "trainMillis", greaterThan(-1L),
                    "configuration", aMapWithSize(9)
                ))
        );
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();

        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }

    @Test
    void trainOnRelationshipFilteredGraph() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('pipe', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 1}, {penalty: 2}] )");

        String trainQuery =
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337, relationshipTypes: $relFilter }" +
            ")";

        Map<String, Object> firstModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        runQuery("CALL gds.beta.model.drop('trainedModel')");

        Map<String, Object> secondModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("REL")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        assertThat(firstModelInfo).usingRecursiveComparison().isNotEqualTo(secondModelInfo);
    }

    @Test
    void trainIsDeterministic() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('pipe', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 0}] )");

        String trainQuery =
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337, relationshipTypes: $relFilter }" +
            ")";

        Map<String, Object> firstModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        runQuery("CALL gds.beta.model.drop('trainedModel')");

        Map<String, Object> secondModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        assertThat(firstModelInfo).usingRecursiveComparison().isEqualTo(secondModelInfo);
    }

    @Test
    void trainUsesRelationshipWeight() {
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'weight'})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.addFeature('pipe', 'COSINE', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureSplit('pipe', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.alpha.ml.pipeline.linkPrediction.configureParams('pipe', [{penalty: 0}] )");

        String trainQuery =
            "CALL gds.alpha.ml.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337, relationshipTypes: $relFilter }" +
            ")";

        Map<String, Object> firstModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );
        var weights1 = weights();

        runQuery("CALL gds.beta.model.drop('trainedModel')");

        Map<String, Object> secondModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", "weighted_graph", "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        var weights2 = weights();
        assertThat(weights1).doesNotContain(weights2);
    }

    private double[] weights() {
        Stream<Model<?, ?, ?>> allModels = ModelCatalog.getAllModels();
        Model<?, ?, ?> model = allModels.filter(m -> m.name().equals("trainedModel")).findFirst().get();
        LinkLogisticRegressionData data = (LinkLogisticRegressionData) model.data();
        return data.weights().data().data();
    }
}
