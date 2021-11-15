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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.InjectModelCatalog;
import org.neo4j.gds.core.ModelCatalogExtension;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.louvain.LouvainMutateProc;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.PipelineCreateConfig;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.PIPELINE_MODEL_TYPE;

@ModelCatalogExtension
class LinkPredictionTrainPipelineExecutorTest extends BaseProcTest {

    @Neo4jGraph
    private static final String GRAPH =
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
        "" +
        "(a)-[:REL]->(b), " +
        "(a)-[:REL]->(c), " +
        "(b)-[:REL]->(c), " +
        "(c)-[:REL]->(d), " +
        "(e)-[:REL]->(f), " +
        "(f)-[:REL]->(g), " +
        "(h)-[:REL]->(i), " +
        "(j)-[:REL]->(k), " +
        "(k)-[:REL]->(l), " +
        "(m)-[:REL]->(n), " +
        "(n)-[:REL]->(o), " +
        "(a)-[:REL]->(d), " +
        "(b)-[:REL]->(d), " +
        "(e)-[:REL]->(g), " +
        "(j)-[:REL]->(l), " +
        "(m)-[:REL]->(o)";


    public static final String GRAPH_NAME = "g";
    public static final NodeLabel NODE_LABEL = NodeLabel.of("N");

    private GraphStore graphStore;

    @InjectModelCatalog
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("noise", "z", "array"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        modelCatalog.removeAllLoadedModels();
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testProcedureAndLinkFeatures() {
        LinkPredictionPipeline pipeline = new LinkPredictionPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.setTrainingParameterSpace(List.of(
            LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1000000)),
            LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1))
        ));

        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array")));

        var config = LinkPredictionTrainConfig
            .builder()
            .graphName(GRAPH_NAME)
            .modelName("model")
            .pipeline("DUMMY")
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var actualModel = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                config,
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            ).compute();

            assertThat(actualModel.name()).isEqualTo("model");

            assertThat(actualModel.algoType()).isEqualTo(LinkPredictionTrain.MODEL_TYPE);
            assertThat(actualModel.trainConfig()).isEqualTo(config);
            // length of the linkFeatures
            assertThat(actualModel.data().weights().data().totalSize()).isEqualTo(7);

            var customInfo = actualModel.customInfo();
            assertThat(customInfo.metrics().get(LinkMetric.AUCPR).validation())
                .hasSize(2)
                .satisfies(scores ->
                    assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
                );

            assertThat(customInfo.bestParameters())
                .usingRecursiveComparison()
                .isEqualTo(LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1)));
        });
    }

    @Test
    void validateLinkFeatureSteps() {
        var pipeline = new LinkPredictionPipeline();
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "no-property", "no-prop-2")));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("other-no-property")));

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                ImmutableLinkPredictionTrainConfig.builder().modelName("foo").pipeline("bar").build(),
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "Node properties [no-property, no-prop-2, other-no-property] defined in the LinkFeatureSteps do not exist in the graph or part of the pipeline");
        });
    }

    static Stream<Arguments> invalidSplits() {
        return Stream.of(
            Arguments.of(
                LinkPredictionSplitConfig.builder().testFraction(0.01).build(),
                "Test graph contains no relationships. Consider increasing the `testFraction` or provide a larger graph"
            ),
            Arguments.of(
                LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build(),
                "Train graph contains no relationships. Consider increasing the `trainFraction` or provide a larger graph"
            )
        );
    }

    @ParameterizedTest
    @MethodSource("invalidSplits")
    void failOnEmptySplitGraph(LinkPredictionSplitConfig splitConfig, String expectedError) {
        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(splitConfig);

        var linkPredictionTrainConfig = ImmutableLinkPredictionTrainConfig.builder()
            .modelName("foo")
            .pipeline("bar")
            .addNodeLabel(NODE_LABEL.name)
            .build();

        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(expectedError);
        });
    }

    @Test
    void failOnExistingSplitRelTypes() {
        var graphName = "invalidGraph";

        String createQuery = GdsCypher.call()
            .withAnyLabel()
            .withRelationshipType("_TEST_", "REL")
            .withRelationshipType("_TEST_COMPLEMENT_", "REL")
            .graphCreate(graphName)
            .yields();

        runQuery(createQuery);

        var invalidGraphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore();

        var pipeline = new LinkPredictionPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder().build());

        var linkPredictionTrainConfig = ImmutableLinkPredictionTrainConfig.builder()
            .modelName("foo")
            .pipeline("bar")
            .addNodeLabel(NODE_LABEL.name)
            .build();

        TestProcedureRunner.applyOnProcedure(db, LouvainMutateProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller,
                invalidGraphStore,
                graphName,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                    "The relationship types ['_TEST_', '_TEST_COMPLEMENT_'] are in the input graph, but are reserved for splitting.");
        });
    }

    @Test
    void shouldLogProgress() {
        LinkPredictionPipeline pipeline = new LinkPredictionPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfig.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.setTrainingParameterSpace(List.of(LinkLogisticRegressionTrainConfig.of(Map.of("penalty", 1))));

        pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("noise", "z", "array", "degree")));

        var config = LinkPredictionTrainConfig
            .builder()
            .graphName(GRAPH_NAME)
            .modelName("model")
            .pipeline("DUMMY")
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();

        var model = Model.of(
            getUsername(),
            "DUMMY",
            PIPELINE_MODEL_TYPE,
            GraphSchema.empty(),
            new Object(),
            PipelineCreateConfig.of(getUsername()),
            pipeline
        );

        modelCatalog.set(model);

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var log = new TestLog();
            var progressTracker = new TestProgressTracker(
                new LinkPredictionTrainPipelineAlgorithmFactory(caller, db.databaseId(), modelCatalog).progressTask(
                    graphStore.getUnion(),
                    config
                ),
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );
            new LinkPredictionTrainPipelineExecutor(
                pipeline,
                config,
                caller,
                graphStore,
                GRAPH_NAME,
                progressTracker
            ).compute();

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .contains(
                    "Link Prediction Train Pipeline :: Start",
                    "Link Prediction Train Pipeline :: split relationships :: Start",
                    "Link Prediction Train Pipeline :: split relationships 100%",
                    "Link Prediction Train Pipeline :: split relationships :: Finished",
                    "Link Prediction Train Pipeline :: execute node property steps :: Start",
                    "Link Prediction Train Pipeline :: execute node property steps :: step 1 of 1 :: Start",
                    "Link Prediction Train Pipeline :: execute node property steps :: step 1 of 1 100%",
                    "Link Prediction Train Pipeline :: execute node property steps :: step 1 of 1 :: Finished",
                    "Link Prediction Train Pipeline :: execute node property steps :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: extract train features :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: extract train features 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: extract train features :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: select model :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: select model 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: select model :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 1 :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 1 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 1 :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 2 :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 2 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Epoch 2 :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: train best model :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: compute train metrics :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: compute train metrics 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: compute train metrics :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: extract test features :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: extract test features 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: extract test features :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: compute test metrics :: Start",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: compute test metrics 100%",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: compute test metrics :: Finished",
                    "Link Prediction Train Pipeline :: LinkPredictionTrain :: evaluate on test data :: Finished",
                    "Link Prediction Train Pipeline :: Finished"
                );
        });
    }
}
