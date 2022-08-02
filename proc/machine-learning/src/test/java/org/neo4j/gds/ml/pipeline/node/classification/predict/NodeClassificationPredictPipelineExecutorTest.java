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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.TreeNode;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.randomforest.ImmutableRandomForestClassifierData;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfigImpl;
import org.neo4j.gds.test.TestProc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.createClassifierData;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelinePredictProcTestUtil.createModel;

class NodeClassificationPredictPipelineExecutorTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";
    private static final String MODEL_NAME = "model";

    @Neo4jGraph
    static String GDL = "CREATE " +
                        "  (n0:N {a: 1.0, b: 0.8, c: 1})" +
                        ", (n1:N {a: 2.0, b: 1.0, c: 1})" +
                        ", (n2:N {a: 3.0, b: 1.5, c: 1})" +
                        ", (n3:N {a: 0.0, b: 2.8, c: 1})" +
                        ", (n4:N {a: 1.0, b: 0.9, c: 1})" +
                        ", (n1)-[:T]->(n2)" +
                        ", (n3)-[:T]->(n4)" +
                        ", (n1)-[:T]->(n3)" +
                        ", (n2)-[:T]->(n4)";

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "g").graphStore();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldPredict() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("includePredictedProbabilities", true)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(),
                Stream.of(
                    NodeFeatureStep.of("a"),
                    NodeFeatureStep.of("b"),
                    NodeFeatureStep.of("c")
                )
            );

            var weights = new double[]{
                -2.0, -1.0, 3.0,
                -1.5, -1.3, 2.6
            };
            var bias = new double[]{0.0, 0.0};
            var modelData = createClassifierData(weights, bias);

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                modelData,
                LocalIdMap.of(42, 1337)
            );

            var predictionResult = pipelineExecutor.compute();

            assertThat(predictionResult.predictedClasses().size()).isEqualTo(graphStore.nodeCount());
            assertThat(predictionResult.predictedClasses().toArray()).containsOnly(42, 1337);
            assertThat(predictionResult.predictedProbabilities()).isPresent();
            assertThat(predictionResult
                .predictedProbabilities()
                .orElseThrow()
                .size()).isEqualTo(graphStore.nodeCount());

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void shouldPredictWithRandomForest() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("includePredictedProbabilities", true)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(),
                Stream.of(
                    NodeFeatureStep.of("a"),
                    NodeFeatureStep.of("b"),
                    NodeFeatureStep.of("c")
                )
            );

            var root = new TreeNode<>(0);
            var modelData = ImmutableRandomForestClassifierData
                .builder()
                .addDecisionTree(new DecisionTreePredictor<>(root))
                .featureDimension(3)
                .numberOfClasses(2)
                .build();

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                modelData,
                LocalIdMap.of(0, 1)
            );

            var predictionResult = pipelineExecutor.compute();

            assertThat(predictionResult.predictedClasses().size()).isEqualTo(graphStore.nodeCount());
            assertThat(predictionResult.predictedProbabilities()).isPresent();
            assertThat(predictionResult
                .predictedProbabilities()
                .orElseThrow()
                .size()).isEqualTo(graphStore.nodeCount());

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void shouldPredictWithNodePropertySteps() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("includePredictedProbabilities", true)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(NodePropertyStepFactory.createNodePropertyStep(
                    "degree",
                    Map.of("mutateProperty", "degree")
                )),
                Stream.of(
                    NodeFeatureStep.of("a"),
                    NodeFeatureStep.of("b"),
                    NodeFeatureStep.of("c"),
                    NodeFeatureStep.of("degree")
                )
            );

            var weights = new double[]{
                1.0, 1.0, -2.0, -1.0,
                0.0, -1.5, -1.3, 2.6
            };
            var bias = new double[]{3.0, 0.0};
            var modelData = createClassifierData(weights, bias);

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                modelData,
                LocalIdMap.of(0, 1)
            );

            var predictionResult = pipelineExecutor.compute();
            assertThat(predictionResult.predictedClasses().size()).isEqualTo(graphStore.nodeCount());
            assertThat(predictionResult.predictedProbabilities()).isPresent();
            assertThat(predictionResult
                .predictedProbabilities()
                .orElseThrow()
                .size()).isEqualTo(graphStore.nodeCount());

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void progressTracking() {
        var config = new NodeClassificationPredictPipelineBaseConfigImpl(
            "",
            CypherMapWrapper.empty()
                .withEntry("modelName", "model")
                .withEntry("includePredictedProbabilities", true)
                .withEntry("graphName", GRAPH_NAME)
        );

        var pipeline = NodePropertyPredictPipeline.from(
            Stream.of(NodePropertyStepFactory.createNodePropertyStep(
                "degree",
                Map.of("mutateProperty", "degree")
            )),
            Stream.of(
                NodeFeatureStep.of("a"),
                NodeFeatureStep.of("b"),
                NodeFeatureStep.of("c"),
                NodeFeatureStep.of("degree")
            )
        );

        var weights = new double[]{
            1.0, 1.0, -2.0, -1.0,
            0.0, -1.5, -1.3, 2.6
        };
        var bias = new double[]{3.0, 0.0};
        var modelData = createClassifierData(weights, bias);

        var progressTracker = new InspectableTestProgressTracker(
            NodeClassificationPredictPipelineExecutor.progressTask(
                "Node Classification Predict Pipeline",
                pipeline,
                graphStore
            ),
            getUsername(),
            config.jobId()
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                progressTracker,
                modelData,
                LocalIdMap.of(0, 1)
            );

            pipelineExecutor.compute();

            var expectedMessages = new ArrayList<>(List.of(
                "Node Classification Predict Pipeline :: Start",
                "Node Classification Predict Pipeline :: Execute node property steps :: Start",
                "Node Classification Predict Pipeline :: Execute node property steps :: DegreeCentrality :: Start",
                "Node Classification Predict Pipeline :: Execute node property steps :: DegreeCentrality 100%",
                "Node Classification Predict Pipeline :: Execute node property steps :: DegreeCentrality :: Finished",
                "Node Classification Predict Pipeline :: Execute node property steps :: Finished",
                "Node Classification Predict Pipeline :: Node classification predict :: Start",
                "Node Classification Predict Pipeline :: Node classification predict 100%",
                "Node Classification Predict Pipeline :: Node classification predict :: Finished",
                "Node Classification Predict Pipeline :: Finished"
            ));

            assertThat(progressTracker.log().getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(expectedMessages.toArray(String[]::new));
        });
        progressTracker.assertValidProgressEvolution();
    }

    @Test
    void validateFeaturesExistOnGraph() {
        var model = createModel(GRAPH_NAME, getUsername(), 3, List.of("a", "b", "d"));
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var streamConfig = NodeClassificationPredictPipelineBaseConfigImpl.builder()
                .modelUser(getUsername())
                .modelName(MODEL_NAME)
                .graphName(GRAPH_NAME)
                .includePredictedProbabilities(false)
                .build();

            var algo = new NodeClassificationPredictPipelineExecutor(
                model.customInfo().pipeline(),
                streamConfig,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                model.data(),
                LocalIdMap.of(0, 1)
            );

            assertThatThrownBy(algo::compute)
                .hasMessage(
                    "Node properties [d] defined in the feature steps do not exist in the graph or part of the pipeline"
                );
        });
    }

    @Test
    void shouldEstimateMemory() {
        var model = createModel(GRAPH_NAME, getUsername(), 3, List.of("a", "b", "d"));
        var config = new NodeClassificationPredictPipelineBaseConfigImpl.Builder()
            .concurrency(1)
            .graphName(GRAPH_NAME)
            .modelName(model.name())
            .includePredictedProbabilities(true)
            .modelUser("user")
            .build();

        var memoryEstimation = NodeClassificationPredictPipelineExecutor.estimate(model, config, new OpenModelCatalog());
        assertMemoryEstimation(
            () -> memoryEstimation,
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            MemoryRange.of(824)
        );

    }


    @Test
    void shouldEstimateMemoryWithRandomForest() {
        var root = new TreeNode<>(0);
        var modelData = ImmutableRandomForestClassifierData
            .builder()
            .addDecisionTree(new DecisionTreePredictor<>(root))
            .featureDimension(3)
            .numberOfClasses(1)
            .build();

        Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> model = Model.of(
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            NodeClassificationPipelineTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .metrics(ClassificationMetricSpecification.Parser.parse(List.of("F1_MACRO")))
                .graphName(GRAPH_NAME)
                .pipeline("DUMMY")
                .targetProperty("foo")
                .build(),
            NodeClassificationPipelineModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(RandomForestClassifierTrainerConfig.DEFAULT, Map.of(), Map.of()),
                NodePropertyPredictPipeline.EMPTY,
                List.of(0L)
            )
        );

        var config = new NodeClassificationPredictPipelineBaseConfigImpl.Builder()
            .concurrency(1)
            .graphName(GRAPH_NAME)
            .modelName(model.name())
            .includePredictedProbabilities(true)
            .modelUser("user")
            .build();

        assertMemoryEstimation(
            () -> NodeClassificationPredictPipelineExecutor.estimate(model, config, new OpenModelCatalog()),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            MemoryRange.of(352)
        );
    }

    @Test
    void failOnInvalidFeatureDimensions() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = NodeClassificationPredictPipelineBaseConfigImpl.builder()
                .modelUser("")
                .modelName("DUMMY")
                .includePredictedProbabilities(false)
                .graphName(GRAPH_NAME)
                .build();

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(),
                Stream.of("a").map(NodeFeatureStep::of)
            );

            double[] manyWeights = {-1.5, -2, 2.5, -1};
            var bias = new double[] {0.0, 0.0};


            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                NodeClassificationPipelinePredictProcTestUtil.createClassifierData(manyWeights, bias),
                LocalIdMap.of(0, 1)
            );

            assertThatThrownBy(pipelineExecutor::compute)
                .hasMessage("Model expected features ['a'] to have a total dimension of `2`, but got `1`.");
        });
    }
}
