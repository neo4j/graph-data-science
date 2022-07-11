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
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineAddStepProcs;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineAddTrainerMethodProcs;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineConfigureSplitProc;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.model.catalog.ModelDropProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.gds.TestSupport.assertCypherMemoryEstimation;

@Neo4jModelCatalogExtension
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

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            LinkPredictionPipelineTrainProc.class,
            LinkPredictionPipelineCreateProc.class,
            LinkPredictionPipelineAddStepProcs.class,
            LinkPredictionPipelineAddTrainerMethodProcs.class,
            LinkPredictionPipelineConfigureSplitProc.class,
            GraphProjectProc.class,
            ModelDropProc.class
        );

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withRelationshipProperty("weight", DefaultValue.of(1.0))
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .yields();

        String createQueryWeighted = GdsCypher.call("weighted_graph")
            .graphProject()
            .withNodeLabels("N", "Ignore")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withRelationshipType("IGNORED", Orientation.UNDIRECTED)
            .withRelationshipProperty("weight", DefaultValue.of(100000.0))
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);
        runQuery(createQueryWeighted);
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainAModel() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe1')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe1', {validationFolds: 2})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe1', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe1', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe1', {penalty: 1})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe1', {penalty: 2})");

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { " +
            "     targetRelationshipType: 'REL', " +
            "     contextRelationshipTypes: ['*'], " +
            "     sourceNodeLabel: 'N', " +
            "     targetNodeLabel: 'N'," +
            "     pipeline: 'pipe1'," +
            "     modelName: 'trainedModel1'," +
            "     metrics: ['AUCPR', 'OUT_OF_BAG_ERROR']," +
            "     negativeClassWeight: 1.0," +
            "     randomSeed: 1337 }" +
            ")",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of(
                    "modelInfo", Matchers.allOf(
                        Matchers.hasEntry("modelName", "trainedModel1"),
                        Matchers.hasEntry("modelType", "LinkPrediction"),
                        Matchers.hasKey("bestParameters"),
                        Matchers.hasKey("metrics"),
                        Matchers.hasKey("pipeline")
                    ),
                    "modelSelectionStats", Matchers.allOf(
                        Matchers.hasKey("modelCandidates"),
                        Matchers.hasKey("bestParameters")
                    ),
                    "trainMillis", greaterThan(-1L),
                    "configuration", aMapWithSize(14)
                ))
        );

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();

        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }

    @Test
    void failsWhenMissingFeatures() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe2')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe2', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe2')");

        assertError("CALL gds.beta.pipeline.linkPrediction.train(" +
                    "   $graphName, " +
                    "   { pipeline: 'pipe2', modelName: 'trainedModel2', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', sourceNodeLabel: 'N', targetNodeLabel: 'N' }" +
                    ")",
            Map.of("graphName", GRAPH_NAME),
            "Training a Link prediction pipeline requires at least one feature. You can add features with the procedure `gds.beta.pipeline.linkPrediction.addFeature`.");
    }

    @Test
    void failsWhenMissingNodeProperty() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe')");
        runQuery(
            "CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'l2', {nodeProperties: ['missingNodeProperty']})");
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.train('g', {modelName: 'm', pipeline: 'pipe', concurrency:2, targetRelationshipType: 'REL', sourceNodeLabel: 'N', targetNodeLabel: 'N'})",
            "Node properties [missingNodeProperty] defined in the feature steps do not exist in the graph or part of the pipeline"
        );
    }

    @Test
    void trainOnNodeLabelFilteredGraph() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe4')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe4', {validationFolds: 2})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe4', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe4', 'L2', {nodeProperties: ['array', 'pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe4', {penalty: 1})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe4', {penalty: 2})");

        Object expectedMetrics = Map.of("AUCPR", Map.of(
            "outerTrain", 1.0,
            "test", 1.0,
            "validation", Map.of("min", 0.0, "avg", 0.5, "max", 1.0),
            "train", Map.of("min", 0.0, "avg", 0.5, "max", 1.0)
        ));

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe4', modelName: 'trainedModel4', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', sourceNodeLabel: 'N', targetNodeLabel: 'N' }" +
            ")",
            Map.of("graphName", GRAPH_NAME),
            List.of(
                Map.of(
                    "modelInfo", Matchers.allOf(
                        Matchers.hasEntry("modelName", "trainedModel4"),
                        Matchers.hasEntry("modelType", "LinkPrediction"),
                        Matchers.hasEntry("metrics", expectedMetrics),
                        Matchers.hasKey("bestParameters"),
                        Matchers.hasKey("pipeline")
                    ),
                    "modelSelectionStats", Matchers.allOf(
                        Matchers.hasKey("modelCandidates"),
                        Matchers.hasKey("bestParameters")
                    ),
                    "trainMillis", greaterThan(-1L),
                    "configuration", aMapWithSize(14)
                ))
        );
        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();

        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("N"))).doesNotContain("pr");
        assertThat(graphStore.nodePropertyKeys(NodeLabel.of("Ignore"))).doesNotContain("pr");
    }

    @Test
    void trainOnRelationshipFilteredGraph() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe5')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe5', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe5', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe5', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe5', {penalty: 1})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe5', {penalty: 2})");

        String trainQuery =
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe5', modelName: 'trainedModel5', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', contextRelationshipTypes: $relFilter, sourceNodeLabel: 'N', targetNodeLabel: 'N' }" +
            ")";

        Map<String, Object> firstModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        runQuery("CALL gds.beta.model.drop('trainedModel5')");

        Map<String, Object> secondModelInfo = runQuery(
            trainQuery,
            Map.of("graphName", GRAPH_NAME, "relFilter", List.of("REL")),
            row -> (Map<String, Object>) row.next().get("modelInfo")
        );

        assertThat(firstModelInfo).usingRecursiveComparison().isNotEqualTo(secondModelInfo);
    }

    @Test
    void trainIsDeterministic() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe6')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe6', 'pageRank', {mutateProperty: 'pr'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe6', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe6', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe6', {penalty: 0, maxEpochs: 10, minEpochs: 10})");

        String trainQuery =
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe6', modelName: 'trainedModel6', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', sourceNodeLabel: 'N', targetNodeLabel: 'N' }" +
            ")";

        runQuery(trainQuery, Map.of("graphName", GRAPH_NAME));
        var data1 = modelData("trainedModel6");

        runQuery("CALL gds.beta.model.drop('trainedModel6')");

        runQuery(trainQuery, Map.of("graphName", GRAPH_NAME, "relFilter", List.of("*")));

        assertThat(data1).usingRecursiveComparison().ignoringFieldsOfTypes(LocalIdMap.class).isEqualTo(modelData("trainedModel6"));
        assertThat(data1.numberOfClasses()).isEqualTo(modelData("trainedModel6").numberOfClasses());
    }

    @Test
    void trainUsesRelationshipWeight() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe7')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe7', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'weight'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe7', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe7', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe7', {penalty: 0, maxEpochs: 10, minEpochs: 10})");

        String trainQuery =
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, " +
            "   { pipeline: 'pipe7', modelName: 'trainedModel7', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', sourceNodeLabel: 'N', targetNodeLabel: 'N' }" +
            ")";

        runQuery(trainQuery, Map.of("graphName", GRAPH_NAME));
        var data1 = modelData("trainedModel7");

        runQuery("CALL gds.beta.model.drop('trainedModel7')");

        runQuery(trainQuery, Map.of("graphName", "weighted_graph", "relFilter", List.of("*")));

        assertThat(data1).usingRecursiveComparison().isNotEqualTo(modelData("trainedModel7"));
    }

    @Test
    void estimate() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'weight'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe')");

        var query = "CALL gds.beta.pipeline.linkPrediction.train.estimate(" +
            "   $graphName, " +
            "   { pipeline: 'pipe', modelName: 'trainedModel', negativeClassWeight: 1.0, randomSeed: 1337, targetRelationshipType: 'REL', contextRelationshipTypes: ['*'], sourceNodeLabel: '*', targetNodeLabel: '*'}" +
            ") YIELD bytesMin, bytesMax, nodeCount, relationshipCount";
        assertCypherMemoryEstimation(
            db,
            query,
            Map.of("graphName", GRAPH_NAME),
            MemoryRange.of(16_592, 510_512),
            16,
            42
        );
    }

    @Test
    void cannotUseOOBAsMainMetricWithLR() {

        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'weight'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addLogisticRegression('pipe')");
        runQuery("CALL gds.alpha.pipeline.linkPrediction.addRandomForest('pipe', {})");
        assertError(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       targetRelationshipType: 'REL', " +
            "       sourceNodeLabel: 'N', " +
            "       targetNodeLabel: 'N'," +
            "       modelName: $modelName," +
            "       metrics: ['OUT_OF_BAG_ERROR', 'AUCPR']," +
            "       randomSeed: 1" +
            "})",
            Map.of("graphName", GRAPH_NAME, "pipeline", "pipe", "modelName", "anything"),
            "If OUT_OF_BAG_ERROR is used as the main metric (the first one)," +
            " then only RandomForest model candidates are allowed." +
            " Incompatible training methods used are: ['LogisticRegression']"
        );
    }

    @Test
    void canUseOOBAsMainMetricWithRF() {
        runQuery("CALL gds.beta.pipeline.linkPrediction.create('pipe')");
        runQuery("CALL gds.beta.pipeline.linkPrediction.configureSplit('pipe', {trainFraction: 0.45, testFraction: 0.45})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addNodeProperty('pipe', 'pageRank', {mutateProperty: 'pr', relationshipWeightProperty: 'weight'})");
        runQuery("CALL gds.beta.pipeline.linkPrediction.addFeature('pipe', 'L2', {nodeProperties: ['pr']})");
        runQuery("CALL gds.alpha.pipeline.linkPrediction.addRandomForest('pipe', {})");

        assertCypherResult(
            "CALL gds.beta.pipeline.linkPrediction.train(" +
            "   $graphName, {" +
            "       pipeline: $pipeline," +
            "       targetRelationshipType: 'REL', " +
            "       sourceNodeLabel: '*', " +
            "       targetNodeLabel: '*'," +
            "       modelName: $modelName," +
            "       metrics: ['OUT_OF_BAG_ERROR', 'AUCPR']," +
            "       randomSeed: 1" +
            "}) YIELD modelInfo" +
            " RETURN modelInfo.metrics.OUT_OF_BAG_ERROR.validation.min AS min_oob",
            Map.of("graphName", GRAPH_NAME, "pipeline", "pipe", "modelName", "anything"),
            List.of(Map.of("min_oob", 0.4))
        );
    }

    private LogisticRegressionData modelData(String trainedModelName) {
        Stream<Model<?, ?, ?>> allModels = modelCatalog.getAllModels();
        Model<?, ?, ?> model = allModels.filter(m -> m.name().equals(trainedModelName)).findFirst().orElseThrow();
        return (LogisticRegressionData) model.data();
    }
}
