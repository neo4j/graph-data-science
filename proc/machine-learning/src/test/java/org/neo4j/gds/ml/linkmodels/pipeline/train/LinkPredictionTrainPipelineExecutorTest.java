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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.metrics.LinkMetric;
import org.neo4j.gds.ml.metrics.classification.OutOfBagError;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;
import org.neo4j.gds.test.TestMutateProc;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class LinkPredictionTrainPipelineExecutorTest extends BaseProcTest {

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
    public static final NodeLabel NODE_LABEL = NodeLabel.of("N");

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
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("scalar", "array"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
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

        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("patience", 5, "tolerance", 0.00001, "penalty", 100)));
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("patience", 5, "tolerance", 0.00001, "penalty", 1)));

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var config = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("model")
            .graphName(GRAPH_NAME)
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
                GRAPH_NAME,
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
                    assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
                );

            assertThat(customInfo.bestParameters())
                .usingRecursiveComparison()
                .isEqualTo(LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "patience", 5, "tolerance", 0.00001)));
        });
    }

    @Test
    void runWithOnlyOOBError() {
        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(RandomForestClassifierTrainerConfig.DEFAULT);

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var config = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("model")
            .graphName(GRAPH_NAME)
            .metrics(List.of(OutOfBagError.OUT_OF_BAG_ERROR.name()))
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
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            ).compute();

            var actualModel = result.model();
            assertThat(actualModel.customInfo().toMap()).containsEntry("metrics",
                Map.of("OUT_OF_BAG_ERROR", Map.of(
                    "test", 0.75,
                    "validation", Map.of("avg", 1.0, "max", 1.0, "min", 1.0))
                )
            );
            assertThat((Map) actualModel.customInfo().toMap().get("metrics")).containsOnlyKeys("OUT_OF_BAG_ERROR");
        });
    }

    @Test
    void validateLinkFeatureSteps() {
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder().testFraction(0.5).trainFraction(0.5).validationFolds(2).build());
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("scalar", "no-property", "no-prop-2")));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("other-no-property")));

        LinkPredictionTrainConfig trainConfig = LinkPredictionTrainConfigImpl
            .builder()
            .username(getUsername())
            .graphName(GRAPH_NAME)
            .modelName("foo")
            .pipeline("bar")
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                trainConfig,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(executor::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                    "Node properties [no-prop-2, no-property, other-no-property] defined in the feature steps do not exist in the graph or part of the pipeline");
        });
    }

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

    @ParameterizedTest
    @MethodSource("invalidSplits")
    void failOnEmptySplitGraph(LinkPredictionSplitConfig splitConfig, String expectedError) {
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(splitConfig);
        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar")));

        var linkPredictionTrainConfig = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("foo")
            .graphName(GRAPH_NAME)
            .pipeline("bar")
            .nodeLabels(List.of(NODE_LABEL.name))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestMutateProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
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

        var invalidGraphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore();

        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder().build());
        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar")));

        var linkPredictionTrainConfig = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("foo")
            .graphName(graphName)
            .pipeline("bar")
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestMutateProc.class, caller -> {
            var executor = new LinkPredictionTrainPipelineExecutor(
                pipeline,
                linkPredictionTrainConfig,
                caller.executionContext(),
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
        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(0.01)
            .trainFraction(0.7)
            .testFraction(0.2)
            .build());

        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("penalty", 1)));

        pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
            "degree",
            Map.of("mutateProperty", "degree")
        ));
        pipeline.addFeatureStep(new HadamardFeatureStep(List.of("scalar", "array", "degree")));

        var config = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("model")
            .graphName(GRAPH_NAME)
            .pipeline("DUMMY")
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var log = Neo4jProxy.testLog();
            var relationshipCount = config
                .internalRelationshipTypes(graphStore)
                .stream()
                .mapToLong(graphStore::relationshipCount)
                .sum();
            var progressTracker = new TestProgressTracker(
                LinkPredictionTrainPipelineExecutor.progressTask(
                    "Link Prediction Train Pipeline",
                    pipeline,
                    relationshipCount
                ),
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );
            new LinkPredictionTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                progressTracker
            ).compute();

            assertThat(log.getMessages(TestLog.WARN))
                .extracting(removingThreadId())
                .containsExactly(
                    "Link Prediction Train Pipeline :: The specified `testFraction` leads to a very small test set with only 3 relationship(s). " +
                    "Proceeding with such a small set might lead to unreliable results.",
                    "Link Prediction Train Pipeline :: The specified `validationFolds` leads to very small validation sets with only 4 relationship(s). " +
                    "Proceeding with such small sets might lead to unreliable results."
                );

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(keepingFixedNumberOfDecimals(4))
                .contains(
                    "Link Prediction Train Pipeline :: Start",
                    "Link Prediction Train Pipeline :: Split relationships :: Start",
                    "Link Prediction Train Pipeline :: Split relationships 100%",
                    "Link Prediction Train Pipeline :: Split relationships :: Finished",
                    "Link Prediction Train Pipeline :: Execute node property steps :: Start",
                    "Link Prediction Train Pipeline :: Execute node property steps :: DegreeCentrality :: Start",
                    "Link Prediction Train Pipeline :: Execute node property steps :: DegreeCentrality 100%",
                    "Link Prediction Train Pipeline :: Execute node property steps :: DegreeCentrality :: Finished",
                    "Link Prediction Train Pipeline :: Execute node property steps :: Finished",
                    "Link Prediction Train Pipeline :: Train set size is 9",
                    "Link Prediction Train Pipeline :: Test set size is 3",
                    "Link Prediction Train Pipeline :: Extract train features :: Start",
                    "Link Prediction Train Pipeline :: Extract train features 50%",
                    "Link Prediction Train Pipeline :: Extract train features 100%",
                    "Link Prediction Train Pipeline :: Extract train features :: Finished",
                    "Link Prediction Train Pipeline :: Select best model :: Start",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Start",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Method: LogisticRegression, Parameters: {batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=1.0}",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 50%",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 100%",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Main validation metric (AUCPR): 1.0000",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Validation metrics: {AUCPR=1.0}",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Training metrics: {AUCPR=1.0}",
                    "Link Prediction Train Pipeline :: Select best model :: Trial 1 of 1 :: Finished",
                    "Link Prediction Train Pipeline :: Select best model :: Best trial was Trial 1 with main validation metric 1.0000",
                    "Link Prediction Train Pipeline :: Select best model :: Finished",
                    "Link Prediction Train Pipeline :: Train best model :: Start",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 1 with loss 0.6891",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 2 with loss 0.6851",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 3 with loss 0.6812",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 98 with loss 0.4678",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 99 with loss 0.4667",
                    "Link Prediction Train Pipeline :: Train best model :: Epoch 100 with loss 0.4655",
                    "Link Prediction Train Pipeline :: Train best model :: terminated after 100 out of 100 epochs. Initial loss: 0.6931, Last loss: 0.4655. Did not converge",
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
    }

    static Stream<Arguments> estimationsForDiffNodeSteps() {
        var degreeCentr = NodePropertyStepFactory.createNodePropertyStep(
            "degree",
            Map.of("mutateProperty", "degree")
        );

        var fastRP = NodePropertyStepFactory.createNodePropertyStep(
            "fastRP",
            Map.of("mutateProperty", "fastRP", "embeddingDimension", 512)
        );

        return Stream.of(
            Arguments.of("only Degree", List.of(degreeCentr), MemoryRange.of(22_488, 696_728)),
            Arguments.of("only FastRP", List.of(fastRP), MemoryRange.of(6_204_136)),
            Arguments.of("Both", List.of(degreeCentr, fastRP), MemoryRange.of(6_204_136))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("estimationsForDiffNodeSteps")
    void estimateWithDifferentNodePropertySteps(String desc, List<NodePropertyStep> nodePropertySteps, MemoryRange expectedRange) {
        var config = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

        for (NodePropertyStep propertyStep : nodePropertySteps) {
            pipeline.addNodePropertyStep(propertyStep);
        }

        GraphDimensions graphDimensions = pipeline.splitConfig().expectedGraphDimensions(1000, 500);

        var actualRange = LinkPredictionTrainPipelineExecutor
            .estimate(new OpenModelCatalog(), pipeline, config)
            .estimate(graphDimensions, config.concurrency())
            .memoryUsage();

        assertMemoryRange(actualRange, expectedRange);
    }

    @Test
    void failEstimateOnEmptyParameterSpace() {
        var config = LinkPredictionTrainConfigImpl.builder()
            .username(getUsername())
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .build();

        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

        assertThatThrownBy(() -> LinkPredictionTrainPipelineExecutor.estimate(new OpenModelCatalog(), pipeline, config))
            .hasMessage("Need at least one model candidate for training.");
    }
}
