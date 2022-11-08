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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.gdl.ImmutableGraphProjectFromGdlConfig;
import org.neo4j.gds.ml.metrics.LinkMetric;
import org.neo4j.gds.ml.metrics.classification.OutOfBagError;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStepTestUtil.NodeIdPropertyStep;
import org.neo4j.gds.ml.pipeline.ImmutablePipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfigImpl;
import org.neo4j.gds.ml.pipeline.PipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.test.TestMutateProc;
import org.neo4j.gds.test.TestProc;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.Orientation.UNDIRECTED;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStepTestUtil.TestNodePropertyStepWithFixedEstimation;
import static org.neo4j.gds.ml.pipeline.PipelineExecutor.DatasetSplits.FEATURE_INPUT;
import static org.neo4j.gds.ml.pipeline.PipelineExecutor.DatasetSplits.TEST;
import static org.neo4j.gds.ml.pipeline.PipelineExecutor.DatasetSplits.TRAIN;

final class LinkPredictionTrainPipelineExecutorTest {

    static Stream<Arguments> invalidSplits() {
        return Stream.of(
            Arguments.of(
                LinkPredictionSplitConfigImpl.builder().testFraction(0.01).build(),
                "The specified `testFraction` is too low for the current graph. The test set would have 0 relationship(s) but it must have at least 1."
            ),
            Arguments.of(
                LinkPredictionSplitConfigImpl.builder().trainFraction(0.01).testFraction(0.5).build(),
                "The specified `trainFraction` is too low for the current graph. The train set would have 0 relationship(s) but it must have at least 2."
            )
        );
    }

    static Stream<Arguments> estimationsForDiffNodeSteps() {
        var lowMemoryStep = new TestNodePropertyStepWithFixedEstimation(
            "lowMemoryStep",
            MemoryEstimations.of("Fixed", MemoryRange.of(1, 2))
        );
        var highMemoryStep = new TestNodePropertyStepWithFixedEstimation(
            "highMemoryStep",
            MemoryEstimations.of("Fixed", MemoryRange.of(6_000_000))
        );

        return Stream.of(
            Arguments.of("low memory step", List.of(lowMemoryStep), MemoryRange.of(22_200, 696_440)),
            Arguments.of("high memory step", List.of(highMemoryStep), MemoryRange.of(6_000_000)),
            Arguments.of("both", List.of(lowMemoryStep, highMemoryStep), MemoryRange.of(6_000_000))
        );
    }

    @Nested
    final class MonoPartiteTest extends BaseProcTest {

        @Neo4jGraph
        private static final String GRAPH =
            "CREATE " +
            "(a:N {scalar: 0, array: [-1.0, -2.0, 1.0, 1.0, 3.0]}), " +
            "(b:N {scalar: 4, array: [2.0, 1.0, -2.0, 2.0, 1.0]}), " +
            "(c:N {scalar: 0, array: [-3.0, 4.0, 3.0, 3.0, 2.0]}), " +
            "(d:N {scalar: 3, array: [1.0, 3.0, 1.0, -1.0, -1.0]}), " +
            "(e:N {scalar: 1, array: [-2.0, 1.0, 2.0, 1.0, -1.0]}), " +
            "(f:N {scalar: 0, array: [-1.0, -3.0, 1.0, 2.0, 2.0]}), " +
            "(g:N {scalar: 1, array: [3.0, 1.0, -3.0, 3.0, 1.0]}), " +
            "(h:N {scalar: 3, array: [-1.0, 3.0, 2.0, 1.0, -3.0]}), " +
            "(i:N {scalar: 3, array: [4.0, 1.0, 1.0, 2.0, 1.0]}), " +
            "(j:N {scalar: 4, array: [1.0, -4.0, 2.0, -2.0, 2.0]}), " +
            "(k:N {scalar: 0, array: [2.0, 1.0, 3.0, 1.0, 1.0]}), " +
            "(l:N {scalar: 1, array: [-1.0, 3.0, -2.0, 3.0, -2.0]}), " +
            "(m:N {scalar: 0, array: [4.0, 4.0, 1.0, 1.0, 1.0]}), " +
            "(n:N {scalar: 3, array: [1.0, -2.0, 3.0, 2.0, 3.0]}), " +
            "(o:N {scalar: 2, array: [-3.0, 3.0, -1.0, -1.0, 1.0]}), " +
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
        public final NodeLabel NODE_LABEL = NodeLabel.of("N");

        private GraphStore graphStore;

        @BeforeEach
        void setup() throws Exception {
            registerProcedures(
                GraphProjectProc.class,
                GraphStreamNodePropertiesProc.class
            );

            runQuery(GdsCypher.call(GRAPH_NAME)
                .graphProject()
                .withNodeLabel("N")
                .withRelationshipType("REL", UNDIRECTED)
                .withNodeProperties(List.of("scalar", "array"), DefaultValue.DEFAULT)
                .yields());

            graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore();
        }

        @Test
        void testProcedureAndLinkFeatures() {
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(1)
                .trainFraction(0.5)
                .testFraction(0.5)
                .build());

            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
                "patience",
                5,
                "tolerance",
                0.00001,
                "penalty",
                100
            )));
            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
                "patience",
                5,
                "tolerance",
                0.00001,
                "penalty",
                1
            )));

            pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var result = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    config,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                ).compute();

                var actualModel = result.model();
                var logisticRegressionData = (LogisticRegressionData) actualModel.data();

                assertThat(actualModel.name()).isEqualTo("model");

                assertThat(actualModel.algoType()).isEqualTo(LinkPredictionTrainingPipeline.MODEL_TYPE);
                assertThat(actualModel.trainConfig()).isEqualTo(config);
                // length of the linkFeatures
                assertThat(logisticRegressionData.weights().data().totalSize()).isEqualTo(6);

                var customInfo = actualModel.customInfo();
                assertThat(result.trainingStatistics().getValidationStats(LinkMetric.AUCPR))
                    .hasSize(2)
                    .satisfies(scores ->
                        assertThat(scores.get(0).avg()).isNotCloseTo(
                            scores.get(1).avg(),
                            Percentage.withPercentage(0.2)
                        )
                    );

                assertThat(customInfo.bestParameters())
                    .usingRecursiveComparison()
                    .isEqualTo(LogisticRegressionTrainConfig.of(Map.of(
                        "penalty",
                        1,
                        "patience",
                        5,
                        "tolerance",
                        0.00001
                    )));
            });
        }

        @Test
        void runWithOnlyOOBError() {
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(2)
                .trainFraction(0.5)
                .testFraction(0.5)
                .build());

            pipeline.addTrainerConfig(RandomForestClassifierTrainerConfig.DEFAULT);

            pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .metrics(List.of(OutOfBagError.OUT_OF_BAG_ERROR.name()))
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(4242L)
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var result = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    config,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                ).compute();

                var actualModel = result.model();
                assertThat(actualModel.customInfo().toMap()).containsEntry(
                    "metrics",
                    Map.of("OUT_OF_BAG_ERROR", Map.of(
                            "test", 0.6666666666666666,
                            "validation", Map.of("avg", 0.6666666666666666, "max", 0.6666666666666666, "min", 0.6666666666666666)
                        )
                    )
                );
                assertThat((Map) actualModel.customInfo().toMap().get("metrics")).containsOnlyKeys("OUT_OF_BAG_ERROR");
            });
        }

        @Test
        void validateLinkFeatureSteps() {
            var pipeline = new LinkPredictionTrainingPipeline();
            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl
                .builder()
                .testFraction(0.5)
                .trainFraction(0.5)
                .validationFolds(2)
                .build());
            pipeline.addFeatureStep(new HadamardFeatureStep(List.of("scalar", "no-property", "no-prop-2")));
            pipeline.addFeatureStep(new HadamardFeatureStep(List.of("other-no-property")));

            LinkPredictionTrainConfig trainConfig = LinkPredictionTrainConfigImpl
                .builder()
                .modelUser(getUsername())
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .modelName("foo")
                .pipeline("bar")
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var executor = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    trainConfig,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                );

                assertThatThrownBy(executor::compute)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                        "Node properties [no-prop-2, no-property, other-no-property] defined in the feature steps do not exist in the graph or part of the pipeline");
            });
        }

        @ParameterizedTest
        @MethodSource("org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutorTest#invalidSplits")
        void failOnEmptySplitGraph(LinkPredictionSplitConfig splitConfig, String expectedError) {
            var pipeline = new LinkPredictionTrainingPipeline();
            pipeline.setSplitConfig(splitConfig);
            pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar")));

            var linkPredictionTrainConfig = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("foo")
                .graphName(GRAPH_NAME)
                .pipeline("bar")
                .sourceNodeLabel(NODE_LABEL.name)
                .targetNodeLabel(NODE_LABEL.name)
                .targetRelationshipType("REL")
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestMutateProc.class, caller -> {
                var executor = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    linkPredictionTrainConfig,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                );

                assertThatThrownBy(executor::compute)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(expectedError);
            });
        }

        @Test
        void failOnExistingSplitRelTypes() {
            var graphName = "invalidGraph";

            String createQuery = GdsCypher.call(graphName)
                .graphProject()
                .withAnyLabel()
                .withNodeProperty("scalar")
                .withRelationshipType("_TEST_", "REL")
                .withRelationshipType("_TEST_COMPLEMENT_", "REL")
                .yields();

            runQuery(createQuery);

            var invalidGraphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), graphName).graphStore();

            var pipeline = new LinkPredictionTrainingPipeline();
            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder().build());
            pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar")));

            var linkPredictionTrainConfig = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("foo")
                .graphName(graphName)
                .pipeline("bar")
                .targetRelationshipType("_TEST_")
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestMutateProc.class, caller -> {
                var executor = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    linkPredictionTrainConfig,
                    caller.executionContext(),
                    invalidGraphStore,
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
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(0.01)
                .trainFraction(0.7)
                .testFraction(0.2)
                .build());

            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("penalty", 1)));

            pipeline.addNodePropertyStep(new NodeIdPropertyStep(graphStore, "Id property step", "generated_id"));
            pipeline.addFeatureStep(new HadamardFeatureStep(List.of("scalar", "array", "generated_id")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            var relationshipCount = config
                .internalRelationshipTypes(graphStore)
                .stream()
                .mapToLong(graphStore::relationshipCount)
                .sum();
            var progressTracker = new InspectableTestProgressTracker(
                LinkPredictionTrainPipelineExecutor.progressTask(
                    "Link Prediction Train Pipeline",
                    pipeline,
                    relationshipCount
                ),
                getUsername(),
                config.jobId()
            );

            TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    config,
                    caller.executionContext(),
                    graphStore,
                    progressTracker
                ).compute();

                assertThat(progressTracker.log().getMessages(TestLog.WARN))
                    .extracting(removingThreadId())
                    .containsExactly(
                        "Link Prediction Train Pipeline :: The specified `testFraction` leads to a very small test set with only 3 relationship(s). " +
                        "Proceeding with such a small set might lead to unreliable results.",
                        "Link Prediction Train Pipeline :: The specified `validationFolds` leads to very small validation sets with only 4 relationship(s). " +
                        "Proceeding with such small sets might lead to unreliable results."
                    );

                assertThat(progressTracker.log().getMessages(TestLog.INFO))
                    .extracting(removingThreadId())
                    .extracting(keepingFixedNumberOfDecimals(4))
                    .contains(
                        "Link Prediction Train Pipeline :: Start",
                        "Link Prediction Train Pipeline :: Split relationships :: Start",
                        "Link Prediction Train Pipeline :: Split relationships 100%",
                        "Link Prediction Train Pipeline :: Split relationships :: Finished",
                        "Link Prediction Train Pipeline :: Execute node property steps :: Start",
                        "Link Prediction Train Pipeline :: Execute node property steps :: Id property step :: Start",
                        "Link Prediction Train Pipeline :: Execute node property steps :: Id property step 100%",
                        "Link Prediction Train Pipeline :: Execute node property steps :: Id property step :: Finished",
                        "Link Prediction Train Pipeline :: Execute node property steps :: Finished",
                        "Link Prediction Train Pipeline :: Train set size is 9",
                        "Link Prediction Train Pipeline :: Test set size is 3",
                        "Link Prediction Train Pipeline :: Extract train features :: Start",
                        "Link Prediction Train Pipeline :: Extract train features 50%",
                        "Link Prediction Train Pipeline :: Extract train features 100%",
                        "Link Prediction Train Pipeline :: Extract train features :: Finished",
                        "Link Prediction Train Pipeline :: Select best model :: Start",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Start",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Method: LogisticRegression, Parameters: {focusWeight=0.0, batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=1.0}",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 50%",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 100%",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Main validation metric (AUCPR): 1.0000",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Validation metrics: {AUCPR=1.0}",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Training metrics: {AUCPR=1.0}",
                        "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Finished",
                        "Link Prediction Train Pipeline :: Select best model :: Best trial was Trial 1 with main validation metric 1.0000",
                        "Link Prediction Train Pipeline :: Select best model :: Finished",
                        "Link Prediction Train Pipeline :: Train best model :: Start",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 1 with loss 0.6603",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 2 with loss 0.6296",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 3 with loss 0.6007",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 98 with loss 0.1622",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 99 with loss 0.1618",
                        "Link Prediction Train Pipeline :: Train best model :: Epoch 100 with loss 0.1614",
                        "Link Prediction Train Pipeline :: Train best model :: terminated after 100 out of 100 epochs. Initial loss: 0.6931, Last loss: 0.1614. Did not converge",
                        "Link Prediction Train Pipeline :: Train best model 100%",
                        "Link Prediction Train Pipeline :: Train best model :: Finished",
                        "Link Prediction Train Pipeline :: Compute train metrics :: Start",
                        "Link Prediction Train Pipeline :: Compute train metrics 100%",
                        "Link Prediction Train Pipeline :: Compute train metrics :: Finished",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Start",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Extract test features :: Start",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Extract test features 100%",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Extract test features :: Finished",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Compute test metrics :: Start",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Compute test metrics 100%",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Compute test metrics :: Finished",
                        "Link Prediction Train Pipeline :: Evaluate on test data :: Finished",
                        "Link Prediction Train Pipeline :: Final model metrics on test set: {AUCPR=1.0}",
                        "Link Prediction Train Pipeline :: Final model metrics on full train set: {AUCPR=1.0000}",
                        "Link Prediction Train Pipeline :: Finished"
                    );
            });
            progressTracker.assertValidProgressEvolution();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutorTest#estimationsForDiffNodeSteps")
        void estimateWithDifferentNodePropertySteps(
            String desc,
            List<ExecutableNodePropertyStep> nodePropertySteps,
            MemoryRange expectedRange
        ) {
            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("DUMMY")
                .graphName("DUMMY")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .build();

            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();
            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

            for (ExecutableNodePropertyStep propertyStep : nodePropertySteps) {
                pipeline.addNodePropertyStep(propertyStep);
            }

            GraphDimensions graphDimensions = pipeline.splitConfig().expectedGraphDimensions(
                GraphDimensions.of(1_000, 500),
                config.targetRelationshipType()
            );

            var actualRange = LinkPredictionTrainPipelineExecutor
                .estimate(ImmutableExecutionContext.EMPTY, pipeline, config)
                .estimate(graphDimensions, config.concurrency())
                .memoryUsage();

            assertMemoryRange(actualRange, expectedRange);
        }

        @Test
        void failEstimateOnEmptyParameterSpace() {
            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("DUMMY")
                .graphName("DUMMY")
                .pipeline("DUMMY")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .build();

            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            assertThatThrownBy(() -> LinkPredictionTrainPipelineExecutor.estimate(
                ExecutionContext.EMPTY,
                pipeline,
                config
            ))
                .hasMessage("Need at least one model candidate for training.");
        }

        @Test
        void shouldValidNodePropertyStepsContextConfigs() {
            var nodePropertyStepGrapFilter = ImmutablePipelineGraphFilter.builder()
                .nodeLabels(List.of(NodeLabel.of("N")))
                .relationshipTypes(List.of(RelationshipType.of("REL")))
                .build();
            var nodePropertyStepInvalidContextNodeLabel = NodePropertyStepContextConfigImpl.builder()
                .contextNodeLabels(List.of("INVALID"))
                .build();
            var nodePropertyStepInvalidContextRel = NodePropertyStepContextConfigImpl.builder()
                .contextRelationshipTypes(List.of("INVALID"))
                .build();


            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();
            pipeline.addNodePropertyStep(new TestFilteredNodePropertyStep(nodePropertyStepGrapFilter, nodePropertyStepInvalidContextNodeLabel));
            pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

            LinkPredictionTrainingPipeline pipeline2 = new LinkPredictionTrainingPipeline();
            pipeline2.addNodePropertyStep(new TestFilteredNodePropertyStep(nodePropertyStepGrapFilter, nodePropertyStepInvalidContextRel));
            pipeline2.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));


            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            assertThatThrownBy(() -> TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var result = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    config,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                ).compute();
            }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not find the specified contextNodeLabels for step `assert step filter` of ['INVALID']. Available labels are ['N'].");


            assertThatThrownBy(() -> TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var result = new LinkPredictionTrainPipelineExecutor(
                    pipeline2,
                    config,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                ).compute();
            }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Could not find the specified contextRelationshipTypes for step `assert step filter` of ['INVALID']. Available relationship types are ['REL'].");

        }

    }

    @Nested
    class PythonTest extends BaseProcTest {

        @Neo4jGraph
        private static final String GRAPH =
            "CREATE" +
            "(a: Node {age: 2})," +
            "(b: Node {age: 3})," +
            "(c: Node {age: 2})," +
            "(d: Node {age: 1})," +
            "(e: Node {age: 2})," +
            "(a)-[:REL]->(b)," +
            "(a)-[:REL]->(c)," +
            "(b)-[:REL]->(c)," +
            "(b)-[:REL]->(a)," +
            "(c)-[:REL]->(a)," +
            "(c)-[:REL]->(b)";

        public static final String GRAPH_NAME = "G";

        private GraphStore graphStore;

        @BeforeEach
        void setup() throws Exception {
            registerProcedures(
                GraphProjectProc.class,
                GraphStreamNodePropertiesProc.class
            );

            runQuery(GdsCypher.call(GRAPH_NAME)
                .graphProject()
                .withNodeLabel("Node")
                .withRelationshipType("REL", UNDIRECTED)
                .withNodeProperties(List.of("age"), DefaultValue.DEFAULT)
                .yields());

            graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), GRAPH_NAME).graphStore();
        }

        @Test
        void pygraph() {
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                //0.33 fails
                .trainFraction(0.34)
                .testFraction(0.2)
                .build());

            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
                "penalty",
                1
            )));

            pipeline.addNodePropertyStep(new NodeIdPropertyStep(graphStore, "degree", "rank"));

            pipeline.addFeatureStep(new L2FeatureStep(List.of("rank")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(getUsername())
                .modelName("model")
                .graphName(GRAPH_NAME)
                .targetRelationshipType("REL")
                .sourceNodeLabel("Node")
                .targetNodeLabel("Node")
                .pipeline("pipe")
                .randomSeed(42L)
                .build();

            TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
                var result = new LinkPredictionTrainPipelineExecutor(
                    pipeline,
                    config,
                    caller.executionContext(),
                    graphStore,
                    ProgressTracker.NULL_TRACKER
                ).compute();
            });

        }
    }

    @Nested
    @GdlExtension
    class BiPartiteTest {

        @GdlGraph(orientation = UNDIRECTED)
        private static final String GRAPH =
            "CREATE " +
            "(p1:P {height: 44})," +
            "(p2:P {height: 111})," +
            "(p3:P {height: 334})," +
            "(p4:P {height: 789})," +
            "(p5:P {height: 123})," +

            "(q1:Q {height: 5})," +
            "(q2:Q {height: 8})," +
            "(q3:Q {height: 334})," +
            "(q4:Q {height: 12})," +
            "(q5:Q {height: 50})," +

            "(x1:X {height: 50})," +
            "(x2:X {height: 50})," +

            "(y1:Y)," +

            "(p1)-[:REL2]->(q1)," +
            "(p1)-[:REL2]->(q3)," +
            "(p1)-[:REL2]->(q5)," +
            "(p2)-[:REL2]->(q3)," +
            "(p2)-[:REL2]->(q4)," +
            "(p2)-[:REL2]->(q5)," +
            "(p4)-[:REL2]->(q5)," +
            "(p3)-[:REL2]->(q4)," +

            "(p1)-[:CONTEXT]->(x1)," +
            "(p3)-[:CONTEXT]->(x2)," +

            "(p1)-[:CONTEXT]->(p3)," +
            "(q1)-[:CONTEXT]->(q1)," +
            "(q1)-[:CONTEXT]->(q4)," +
            "(p1)-[:CONTEXT]->(p4)," +

            "(x1)-[:CONTEXT]->(y1)";
        private static final String G_BI = "g_bi";

        @Inject
        private GraphStore graphStore;
        private final String username = "alice";

        @BeforeEach
        void setUp() {
            var graphConfig = ImmutableGraphProjectFromGdlConfig
                .builder()
                .gdlGraph(G_BI)
                .graphName("first")
                .username(username)
                .build();

            GraphStoreCatalog.set(graphConfig, graphStore);
        }

        @Test
        void withAdvancedFiltering() {
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(1)
                .trainFraction(0.5)
                .testFraction(0.5)
                .build());

            pipeline.addNodePropertyStep(new TestFilteredNodePropertyStep(
                ImmutablePipelineGraphFilter.builder()
                    .nodeLabels(List.of(NodeLabel.of("P"), NodeLabel.of("Q"), NodeLabel.of("X")))
                    .relationshipTypes(List.of(RelationshipType.of("_FEATURE_INPUT_")))
                    .build(),
                NodePropertyStepContextConfigImpl.builder()
                    .contextNodeLabels(List.of("X"))
                    .contextRelationshipTypes(List.of("CONTEXT")).build()));

            pipeline.addTrainerConfig(RandomForestClassifierTrainerConfig.DEFAULT);

            pipeline.addFeatureStep(new L2FeatureStep(List.of("height")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(username)
                .modelName("model")
                .graphName(G_BI)
                .targetRelationshipType("REL2")
                .sourceNodeLabel("P")
                .targetNodeLabel("Q")
                .metrics(List.of(LinkMetric.AUCPR.name()))
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            var result = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                config,
                ExecutionContext.EMPTY,
                graphStore,
                ProgressTracker.NULL_TRACKER
            ).compute();

            // mainly a smoke test
            assertThat(result.trainingStatistics().winningModelOuterTrainMetrics().get(LinkMetric.AUCPR))
                .isCloseTo(0.375, Offset.offset(1e-3)
            );
        }

        @Test
        void splitsRespectTrainConfigFiltering() {
            var pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(1)
                .trainFraction(0.5)
                .testFraction(0.5)
                .build());
            pipeline.addTrainerConfig(RandomForestClassifierTrainerConfig.DEFAULT);
            pipeline.addFeatureStep(new L2FeatureStep(List.of("height")));

            var config = LinkPredictionTrainConfigImpl.builder()
                .modelUser(username)
                .modelName("model")
                .graphName(G_BI)
                .targetRelationshipType("REL2")
                .sourceNodeLabel("P")
                .targetNodeLabel("Q")
                .metrics(List.of(LinkMetric.AUCPR.name()))
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            var splits = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                config,
                ExecutionContext.EMPTY,
                graphStore,
                ProgressTracker.NULL_TRACKER
            ).generateDatasetSplitGraphFilters();

            assertThat(splits.get(FEATURE_INPUT).nodeLabels()).containsExactlyInAnyOrder(
                NodeLabel.of("P"),
                NodeLabel.of("Q")
            );
            assertThat(splits.get(TEST).nodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("P"), NodeLabel.of("Q"));
            assertThat(splits.get(TRAIN).nodeLabels()).containsExactlyInAnyOrder(NodeLabel.of("P"), NodeLabel.of("Q"));
        }


        @Test
        void validateNodePropertiesExistOnNodesInScope() {
            LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

            pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
                .validationFolds(2)
                .negativeSamplingRatio(1)
                .trainFraction(0.5)
                .testFraction(0.5)
                .build());

            var trainConfig = LinkPredictionTrainConfigImpl.builder()
                .modelUser(username)
                .modelName("model")
                .graphName(G_BI)
                .targetRelationshipType("REL2")
                .sourceNodeLabel("P")
                .targetNodeLabel("Y")
                .metrics(List.of(LinkMetric.AUCPR.name()))
                .pipeline("DUMMY")
                .negativeClassWeight(1)
                .randomSeed(1337L)
                .build();

            pipeline.addNodePropertyStep(new TestFilteredNodePropertyStep(
                ImmutablePipelineGraphFilter.builder()
                    .nodeLabels(trainConfig.nodeLabelIdentifiers(graphStore))
                    .relationshipTypes(List.of(RelationshipType.of("_FEATURE_INPUT_")))
                    .build(),
                NodePropertyStepContextConfigImpl.builder()
                    .contextNodeLabels(List.of("X"))
                    .contextRelationshipTypes(List.of("CONTEXT"))
                    .build())
            );

            pipeline.addTrainerConfig(RandomForestClassifierTrainerConfig.DEFAULT);

            pipeline.addFeatureStep(new L2FeatureStep(List.of("height")));

            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                trainConfig,
                ExecutionContext.EMPTY,
                graphStore,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Node properties [height] defined in the feature steps do not exist in the graph or part of the pipeline");
        }
    }


    static class TestFilteredNodePropertyStep implements ExecutableNodePropertyStep {
        private final PipelineGraphFilter graphFilter;

        private final NodePropertyStepContextConfig nodePropertyStepContextConfig;

        TestFilteredNodePropertyStep(PipelineGraphFilter graphFilter, NodePropertyStepContextConfig nodePropertyStepContextConfig) {
            this.graphFilter = graphFilter;
            this.nodePropertyStepContextConfig = nodePropertyStepContextConfig;
        }

        @Override
        public List<String> contextNodeLabels() {
            return nodePropertyStepContextConfig.contextNodeLabels();
        }

        @Override
        public List<String> contextRelationshipTypes() {
            return nodePropertyStepContextConfig.contextRelationshipTypes();
        }

        @Override
        public void execute(
            ExecutionContext executionContext,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes
        ) {
            assertThat(nodeLabels).containsExactlyInAnyOrderElementsOf(
                Stream.concat(graphFilter.nodeLabels().stream(), contextNodeLabels().stream().map(NodeLabel::of)).distinct().collect(Collectors.toList())
            );
            assertThat(relTypes).containsExactlyInAnyOrderElementsOf(
                Stream.concat(graphFilter.relationshipTypes().stream(), contextRelationshipTypes().stream().map(RelationshipType::of)).distinct().collect(Collectors.toList())
            );
        }

        @Override
        public Map<String, Object> config() {
            return Map.of(MUTATE_PROPERTY_KEY, "test");
        }

        @Override
        public String procName() {
            return "assert step filter";
        }

        @Override
        public MemoryEstimation estimate(
            ModelCatalog modelCatalog,
            String username,
            List<String> nodeLabels,
            List<String> relTypes
        ) {
            return MemoryEstimations.of("fake", MemoryRange.of(0));
        }

        @Override
        public String mutateNodeProperty() {
            return "test";
        }

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }
    }
}
