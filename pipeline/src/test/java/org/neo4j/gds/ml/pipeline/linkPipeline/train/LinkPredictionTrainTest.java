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

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfigImpl;
import org.neo4j.gds.ml.pipeline.AutoTuningConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.metrics.LinkMetric.AUCPR;
import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;

@GdlExtension
class LinkPredictionTrainTest {

    static String NODES = "(a:N {scalar: 0, array: [-1.0, -2.0, 1.0, 1.0, 3.0]}), " +
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
                          "(o:N {scalar: 2, array: [-3.0, 3.0, -1.0, -1.0, 1.0]}), ";

    @GdlGraph(graphNamePrefix = "train", idOffset = 42)
    static String GRAPH =
        "CREATE " +
        NODES +
        "(a)-[:REL {label: 1.0}]->(b), " +
        "(a)-[:REL {label: 1.0}]->(c), " +
        "(a)-[:REL {label: 0.0}]->(e), " +
        "(a)-[:REL {label: 1.0}]->(h), " +
        "(a)-[:REL {label: 1.0}]->(i), " +
        "(a)-[:REL {label: 0.0}]->(i), " +
        "(b)-[:REL {label: 0.0}]->(c), " +
        "(b)-[:REL {label: 1.0}]->(f), " +
        "(b)-[:REL {label: 1.0}]->(g), " +
        "(b)-[:REL {label: 0.0}]->(n), " +
        "(b)-[:REL {label: 1.0}]->(o), " +
        "(c)-[:REL {label: 1.0}]->(d), " +
        "(c)-[:REL {label: 1.0}]->(h), " +
        "(c)-[:REL {label: 1.0}]->(l), " +
        "(e)-[:REL {label: 0.0}]->(f), " +
        "(e)-[:REL {label: 0.0}]->(a), " +
        "(f)-[:REL {label: 0.0}]->(g), " +
        "(f)-[:REL {label: 0.0}]->(o), " +
        "(h)-[:REL {label: 0.0}]->(i), " +
        "(j)-[:REL {label: 1.0}]->(k), " +
        "(k)-[:REL {label: 0.0}]->(l), " +
        "(m)-[:REL {label: 0.0}]->(n), " +
        "(n)-[:REL {label: 0.0}]->(o) ";

    @Inject
    Graph trainGraph;

    static Stream<Arguments> paramsForEstimationsWithSplitConfigs() {
        return Stream.of(
            Arguments.of(
                "Default",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.1).validationFolds(2).build(),
                MemoryRange.of(27_504, 897_744)
            ),
            Arguments.of(
                "Higher test-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.6).trainFraction(0.1).validationFolds(2).build(),
                MemoryRange.of(76_704, 2_906_944)
            ),
            Arguments.of(
                "Higher train-set",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(2).build(),
                MemoryRange.of(119_216, 3_949_056)
            ),
            Arguments.of(
                "Higher validation folds",
                LinkPredictionSplitConfigImpl.builder().testFraction(0.1).trainFraction(0.6).validationFolds(5).build(),
                MemoryRange.of(132_416, 3_962_256)
            )
        );
    }

    static Stream<Arguments> paramsForEstimationsWithParamSpace() {
        var llrConfigs = Stream.of(
                Map.<String, Object>of("batchSize", 10),
                Map.<String, Object>of("batchSize", 100L)
            )
            .map(LogisticRegressionTrainConfig::of)
            .map(TrainerConfig::toTunableConfig)
            .collect(Collectors.toList());

        return Stream.of(
            Arguments.of("LLR batchSize 10",
                List.of(llrConfigs.get(0)), MemoryRange.of(22_736, 716_576)
            ),
            Arguments.of(
                "LLR batchSize 100",
                List.of(llrConfigs.get(1)),
                MemoryRange.of(28_304, 898_544)
            ),
            Arguments.of(
                "LLR batchSize 10,100",
                llrConfigs,
                MemoryRange.of(28_384, 898_624)
            ),
            Arguments.of(
                "RF",
                List.of(RandomForestClassifierTrainerConfigImpl
                    .builder()
                    .maxDepth(3)
                    .minSplitSize(2)
                    .maxFeaturesRatio(1.0D)
                    .numberOfDecisionTrees(1)
                    .build()
                    .toTunableConfig()
                ),
                MemoryRange.of(35_720, 473_320)
            ),
            Arguments.of(
                "Default RF and default LR",
                List.of(
                    LogisticRegressionTrainConfig.DEFAULT.toTunableConfig(),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(42_800, 1_371_008)
            ),
            Arguments.of(
                "Default RF and default LR with range",
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("penalty", Map.of("range", List.of(1e-4, 1e4))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(42_800, 1_371_008)
            ),
            Arguments.of(
                "Default RF and default LR with batch size range",
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("batchSize", Map.of("range", List.of(1, 100_000))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(12_815_584, 405_293_824)
            )
        );
    }

    @Test
    void trainsAModel() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var result = runLinkPrediction(trainConfig);

        assertThat(result.trainingStatistics().getTrainStats(AUCPR).size()).isEqualTo(MAX_TRIALS);

        var trainedClassifier = result.classifier();

        assertThat((LogisticRegressionData) trainedClassifier.data())
            .extracting(llrData -> llrData.weights().data().totalSize())
            .isEqualTo(6);

        assertThat(result.trainingStatistics().getValidationStats(AUCPR))
            .satisfies(scores ->
                assertThat(scores.get(0).avg()).isNotCloseTo(scores.get(1).avg(), Percentage.withPercentage(0.2))
            );

        assertThat(result.trainingStatistics().bestParameters())
            .usingRecursiveComparison()
            .isEqualTo(LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "patience", 5, "tolerance", 0.00001)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldProduceCorrectTrainingStatistics(boolean includeOOB) {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(
            modelName,
            includeOOB ? List.of(AUCPR, OUT_OF_BAG_ERROR) : List.of(AUCPR)
        );

        var result = runLinkPrediction(trainConfig);

        var trainingStatistics = result.trainingStatistics();

        assertThat(trainingStatistics.getBestTrialIdx()).isBetween(0, 10);
        assertThat(trainingStatistics.getValidationStats(AUCPR))
            .hasSize(10)
            .noneMatch(Objects::isNull);
        assertThat(trainingStatistics.getTrainStats(AUCPR))
            .hasSize(10)
            .noneMatch(Objects::isNull);

        assertThat(trainingStatistics.winningModelOuterTrainMetrics()).containsKeys(AUCPR);
        assertThat(trainingStatistics.winningModelTestMetrics()).containsOnlyKeys(AUCPR);

        if (includeOOB) {
            assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).hasSize(10);
            assertThat(trainingStatistics.getTrainStats(OUT_OF_BAG_ERROR)).containsOnlyNulls();
        } else {
            assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).containsOnlyNulls();
        }
    }

    @Test
    void shouldProduceCorrectTrainingStatisticsForWinningRF() {
        String modelName = "model";

        var trainConfig = trainingConfig(modelName, List.of(OUT_OF_BAG_ERROR));

        var pipeline = new LinkPredictionTrainingPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio", Map.of("range", List.of(0.05, 0.1))
                ),
                TrainingMethod.RandomForestClassification
            ));
        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var linkPredictionTrain = new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            trainConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var trainingStatistics = linkPredictionTrain.compute().trainingStatistics();

        assertThat(trainingStatistics.getValidationStats(OUT_OF_BAG_ERROR)).hasSize(10).noneMatch(Objects::isNull);
        assertThat(trainingStatistics.getTrainStats(OUT_OF_BAG_ERROR)).hasSize(10).containsOnlyNulls();

        assertThat(trainingStatistics.winningModelOuterTrainMetrics()).isEmpty();
        assertThat(trainingStatistics.winningModelTestMetrics()).containsOnlyKeys(OUT_OF_BAG_ERROR);
    }

    @Test
    void seededTrain() {
        String modelName = "model";

        LinkPredictionTrainConfig trainConfig = trainingConfig(modelName);

        var modelData = runLinkPrediction(trainConfig)
            .classifier()
            .data();
        var modelDataRepeated = runLinkPrediction(trainConfig)
            .classifier()
            .data();

        assertThat(modelData)
            .usingRecursiveComparison()
            .withEqualsForType(LocalIdMap::equals, LocalIdMap.class)
            .isEqualTo(modelDataRepeated);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  10,   10, 2_640, 69_280",
        "  10,  100, 17_336, 534_776",
        "  10, 1000, 28_304, 898_544",
        // nodeCount has no effect on the estimation
        "1000, 1000, 28_304, 898_544"
    })
    void estimateWithDifferentGraphSizes(int nodeCount, int relationshipCount, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfigImpl.builder()
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .targetRelationshipType("REL")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .build();

        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

        var graphDim = GraphDimensions.of(nodeCount, relationshipCount);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig(), trainConfig), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertMemoryRange(actualRange, expectedMinEstimation, expectedMaxEstimation);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithSplitConfigs")
    void estimateWithDifferentSplits(String desc, LinkPredictionSplitConfig splitConfig, MemoryRange expectedRange) {
        var trainConfig = LinkPredictionTrainConfigImpl.builder()
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .targetRelationshipType("REL")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .build();

        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);
        pipeline.setSplitConfig(splitConfig);

        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig(), trainConfig), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertMemoryRange(actualRange, expectedRange.min, expectedRange.max);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("paramsForEstimationsWithParamSpace")
    void estimateWithParameterSpace(String desc, List<TunableTrainerConfig> tunableConfigs, MemoryRange expectedRange) {
        var trainConfig = LinkPredictionTrainConfigImpl.builder()
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .pipeline("DUMMY")
            .targetRelationshipType("REL")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .build();

        var pipeline = new LinkPredictionTrainingPipeline();
        for (var config: tunableConfigs) {
            pipeline.addTrainerConfig(config);
        }

        // Limit maxTrials to make comparison with concrete-only parameter spaces easier.
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(2).build());

        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig(), trainConfig), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertMemoryRange(actualRange, expectedRange.min, expectedRange.max);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "  1,  27_776, 874_496",
        "  2,  27_952, 882_512",
        "  4,  28_304, 898_544",
    })
    void estimateWithConcurrency(int concurrency, int expectedMinEstimation, int expectedMaxEstimation) {
        var trainConfig = LinkPredictionTrainConfigImpl.builder()
            .username("DUMMY")
            .modelName("DUMMY")
            .graphName("DUMMY")
            .targetRelationshipType("REL")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .pipeline("DUMMY")
            .concurrency(concurrency)
            .build();

        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.DEFAULT);

        var graphDim = GraphDimensions.of(100, 1_000);
        MemoryTree actualEstimation = LinkPredictionTrain
            .estimate(pipeline, trainConfig)
            .estimate(graphDimensionsWithSplits(graphDim, pipeline.splitConfig(), trainConfig), trainConfig.concurrency());

        MemoryRange actualRange = actualEstimation.memoryUsage();
        assertMemoryRange(actualRange, expectedMinEstimation, expectedMaxEstimation);
    }

    @Test
    void logProgressRF() {
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(
            RandomForestClassifierTrainerConfigImpl.builder().numberOfDecisionTrees(5).build()
        );

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            progressTask(
                trainGraph.relationshipCount(),
                pipeline.splitConfig(),
                pipeline.numberOfModelSelectionTrials()
            ),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        progressTracker.beginSubTask();
        new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfigImpl.builder()
                .username("DUMMY")
                .modelName("DUMMY")
                .graphName("DUMMY")
                .pipeline("DUMMY")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .randomSeed(42L)
                .concurrency(4)
                .build(),
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        ).compute();
        progressTracker.endSubTask();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals(4))
            .containsExactlyInAnyOrder(
                "MY TEST TASK :: Start",
                "MY TEST TASK :: Extract train features :: Start",
                "MY TEST TASK :: Extract train features 50%",
                "MY TEST TASK :: Extract train features 100%",
                "MY TEST TASK :: Extract train features :: Finished",
                "MY TEST TASK :: Create validation folds :: Start",
                "MY TEST TASK :: Create validation folds 100%",
                "MY TEST TASK :: Create validation folds :: Finished",
                "MY TEST TASK :: Select best model :: Start",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Start",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Method: RandomForest, Parameters: {criterion=GINI, numberOfSamplesRatio=1.0, numberOfDecisionTrees=5, maxDepth=2147483647, minSplitSize=2, minLeafSize=1}",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 50%",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 100%",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Main validation metric (AUCPR): 0.7684",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Validation metrics: {AUCPR=0.7683}",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Training metrics: {AUCPR=0.7804}",
                "MY TEST TASK :: Select best model :: Trial 1 of 1 :: Finished",
                "MY TEST TASK :: Select best model :: Best trial was Trial 1 with main validation metric 0.7684",
                "MY TEST TASK :: Select best model :: Finished",
                "MY TEST TASK :: Train best model :: Start",
                "MY TEST TASK :: Train best model :: Trained decision tree 2 out of 5",
                "MY TEST TASK :: Train best model :: Trained decision tree 1 out of 5",
                "MY TEST TASK :: Train best model :: Trained decision tree 3 out of 5",
                "MY TEST TASK :: Train best model :: Trained decision tree 4 out of 5",
                "MY TEST TASK :: Train best model :: Trained decision tree 5 out of 5",
                "MY TEST TASK :: Train best model 100%",
                "MY TEST TASK :: Train best model :: Finished",
                "MY TEST TASK :: Compute train metrics :: Start",
                "MY TEST TASK :: Compute train metrics 100%",
                "MY TEST TASK :: Compute train metrics :: Finished",
                "MY TEST TASK :: Final model metrics on full train set: {AUCPR=0.7117}",
                "MY TEST TASK :: Evaluate on test data :: Start",
                "MY TEST TASK :: Evaluate on test data :: Extract test features :: Start",
                "MY TEST TASK :: Evaluate on test data :: Extract test features 50%",
                "MY TEST TASK :: Evaluate on test data :: Extract test features 100%",
                "MY TEST TASK :: Evaluate on test data :: Extract test features :: Finished",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics :: Start",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics 100%",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics :: Finished",
                "MY TEST TASK :: Evaluate on test data :: Finished",
                "MY TEST TASK :: Final model metrics on test set: {AUCPR=0.7117}",
                "MY TEST TASK :: Finished"
            );
    }

    @Test
    void logProgressLR() {
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(
            LogisticRegressionTrainConfigImpl.builder().maxEpochs(5).minEpochs(5).build()
        );

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            progressTask(
                trainGraph.relationshipCount(),
                pipeline.splitConfig(),
                pipeline.numberOfModelSelectionTrials()
            ),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        progressTracker.beginSubTask();
        new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfigImpl.builder()
                .username("DUMMY")
                .modelName("DUMMY")
                .graphName("DUMMY")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .pipeline("DUMMY")
                .concurrency(4)
                .build(),
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        ).compute();
        progressTracker.endSubTask();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals(4))
            .contains(
                "MY TEST TASK :: Train best model :: Start",
                "MY TEST TASK :: Train best model :: Initial loss 0.6931",
                "MY TEST TASK :: Train best model :: Epoch 1 with loss 0.6880",
                "MY TEST TASK :: Train best model :: Epoch 2 with loss 0.6832",
                "MY TEST TASK :: Train best model :: Epoch 3 with loss 0.6784",
                "MY TEST TASK :: Train best model :: Epoch 4 with loss 0.6739",
                "MY TEST TASK :: Train best model :: Epoch 5 with loss 0.6695",
                "MY TEST TASK :: Train best model :: Finished"
            );
    }

    @Test
    void logProgressLRWithRange() {
        int MAX_TRIALS = 4;
        var pipeline = new LinkPredictionTrainingPipeline();
        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(TunableTrainerConfig.of(
            Map.of("maxEpochs", 5, "penalty", Map.of("range", List.of(1e-4, 1e4))),
            TrainingMethod.LogisticRegression
        ));
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(MAX_TRIALS).build());

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));

        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            progressTask(
                2 * trainGraph.relationshipCount(),
                pipeline.splitConfig(),
                pipeline.numberOfModelSelectionTrials()
            ),
            log,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        progressTracker.beginSubTask();
        new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            pipeline,
            LinkPredictionTrainConfigImpl
                .builder()
                .username("DUMMY")
                .randomSeed(42L)
                .modelName("DUMMY")
                .graphName("DUMMY")
                .pipeline("DUMMY")
                .targetRelationshipType("REL")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .concurrency(4)
                .build(),
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        ).compute();
        progressTracker.endSubTask();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals(4))
            .containsExactly(
                "MY TEST TASK :: Start",
                "MY TEST TASK :: Extract train features :: Start",
                "MY TEST TASK :: Extract train features 50%",
                "MY TEST TASK :: Extract train features 100%",
                "MY TEST TASK :: Extract train features :: Finished",
                "MY TEST TASK :: Create validation folds :: Start",
                "MY TEST TASK :: Create validation folds 100%",
                "MY TEST TASK :: Create validation folds :: Finished",
                "MY TEST TASK :: Select best model :: Start",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Start",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Method: LogisticRegression, Parameters: {batchSize=100, minEpochs=1, patience=1, maxEpochs=5, tolerance=0.001, learningRate=0.001, penalty=0.0019}",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 50%",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 100%",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Main validation metric (AUCPR): 0.7169",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Validation metrics: {AUCPR=0.7169}",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Training metrics: {AUCPR=0.7011}",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Finished",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Start",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Method: LogisticRegression, Parameters: {batchSize=100, minEpochs=1, patience=1, maxEpochs=5, tolerance=0.001, learningRate=0.001, penalty=0.0566}",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 50%",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 100%",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Main validation metric (AUCPR): 0.7169",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Validation metrics: {AUCPR=0.7169}",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Training metrics: {AUCPR=0.7011}",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Finished",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Start",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Method: LogisticRegression, Parameters: {batchSize=100, minEpochs=1, patience=1, maxEpochs=5, tolerance=0.001, learningRate=0.001, penalty=882.7233}",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 50%",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 100%",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Main validation metric (AUCPR): 0.6812",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Validation metrics: {AUCPR=0.6811}",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Training metrics: {AUCPR=0.8076}",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Finished",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Start",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Method: LogisticRegression, Parameters: {batchSize=100, minEpochs=1, patience=1, maxEpochs=5, tolerance=0.001, learningRate=0.001, penalty=254.1294}",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 50%",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 100%",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Main validation metric (AUCPR): 0.6912",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Validation metrics: {AUCPR=0.6911}",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Training metrics: {AUCPR=0.8000}",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Finished",
                "MY TEST TASK :: Select best model :: Best trial was Trial 1 with main validation metric 0.7169",
                "MY TEST TASK :: Select best model :: Finished",
                "MY TEST TASK :: Train best model :: Start",
                "MY TEST TASK :: Train best model :: Initial loss 0.6931",
                "MY TEST TASK :: Train best model :: Epoch 1 with loss 0.6880",
                "MY TEST TASK :: Train best model :: Epoch 2 with loss 0.6832",
                "MY TEST TASK :: Train best model :: Epoch 3 with loss 0.6784",
                "MY TEST TASK :: Train best model :: Epoch 4 with loss 0.6739",
                "MY TEST TASK :: Train best model :: Epoch 5 with loss 0.6695",
                "MY TEST TASK :: Train best model :: terminated after 5 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6695. Did not converge",
                "MY TEST TASK :: Train best model 100%",
                "MY TEST TASK :: Train best model :: Finished",
                "MY TEST TASK :: Compute train metrics :: Start",
                "MY TEST TASK :: Compute train metrics 100%",
                "MY TEST TASK :: Compute train metrics :: Finished",
                "MY TEST TASK :: Final model metrics on full train set: {AUCPR=0.7808}",
                "MY TEST TASK :: Evaluate on test data :: Start",
                "MY TEST TASK :: Evaluate on test data :: Extract test features :: Start",
                "MY TEST TASK :: Evaluate on test data :: Extract test features 50%",
                "MY TEST TASK :: Evaluate on test data :: Extract test features 100%",
                "MY TEST TASK :: Evaluate on test data :: Extract test features :: Finished",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics :: Start",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics 100%",
                "MY TEST TASK :: Evaluate on test data :: Compute test metrics :: Finished",
                "MY TEST TASK :: Evaluate on test data :: Finished",
                "MY TEST TASK :: Final model metrics on test set: {AUCPR=0.7808}",
                "MY TEST TASK :: Finished"
            );

        assertThat(log.getMessages(TestLog.DEBUG))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals(4))
            .containsExactly(
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Starting fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 1 with loss 0.6883",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 2 with loss 0.6840",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 3 with loss 0.6800",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 4 with loss 0.6762",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 5 with loss 0.6725",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: terminated after 5 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6725. Did not converge",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Finished fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Starting fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 1 with loss 0.6877",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 2 with loss 0.6824",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 3 with loss 0.6773",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 4 with loss 0.6723",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Epoch 5 with loss 0.6674",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: terminated after 5 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6674. Did not converge",
                "MY TEST TASK :: Select best model :: Trial 1 of 4 :: Finished fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Starting fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 1 with loss 0.6883",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 2 with loss 0.6840",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 3 with loss 0.6800",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 4 with loss 0.6762",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 5 with loss 0.6725",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: terminated after 5 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6725. Did not converge",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Finished fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Starting fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 1 with loss 0.6877",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 2 with loss 0.6824",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 3 with loss 0.6773",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 4 with loss 0.6723",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Epoch 5 with loss 0.6674",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: terminated after 5 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6674. Did not converge",
                "MY TEST TASK :: Select best model :: Trial 2 of 4 :: Finished fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Starting fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Epoch 1 with loss 0.6936",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Epoch 2 with loss 0.6916",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Epoch 3 with loss 0.6922",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: converged after 3 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6922.",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Finished fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Starting fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Epoch 1 with loss 0.6930",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Epoch 2 with loss 0.6925",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: converged after 2 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6925.",
                "MY TEST TASK :: Select best model :: Trial 3 of 4 :: Finished fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Starting fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 1 with loss 0.6899",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 2 with loss 0.6873",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 3 with loss 0.6862",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 4 with loss 0.6862",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: converged after 4 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6862.",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Finished fold 1 training",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Starting fold 2 training",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Initial loss 0.6931",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 1 with loss 0.6892",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 2 with loss 0.6875",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Epoch 3 with loss 0.6874",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: converged after 3 out of 5 epochs. Initial loss: 0.6931, Last loss: 0.6874.",
                "MY TEST TASK :: Select best model :: Trial 4 of 4 :: Finished fold 2 training"
            );
    }

    static Task progressTask(long relationshipCount, LinkPredictionSplitConfig splitConfig, int numberOfModelSelectionTrials) {
        return Tasks.task(
            "MY TEST TASK",
            LinkPredictionTrain.progressTasks(relationshipCount, splitConfig, numberOfModelSelectionTrials)
        );
    }

    private LinkPredictionTrainingPipeline linkPredictionPipeline() {
        LinkPredictionTrainingPipeline pipeline = new LinkPredictionTrainingPipeline();

        pipeline.setSplitConfig(LinkPredictionSplitConfigImpl.builder()
            .validationFolds(2)
            .negativeSamplingRatio(1)
            .trainFraction(0.5)
            .testFraction(0.5)
            .build());

        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
            "penalty", 1,
            "patience", 5,
            "tolerance", 0.00001
        )));
        pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of(
            "penalty", 100,
            "patience", 5,
            "tolerance", 0.00001
        )));

        // Should NOT be the winning model, so give it bad hyperparams.
        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio", 0.1
                ),
                TrainingMethod.RandomForestClassification
            ));

        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of(
                    "minSplitSize", 2,
                    "maxDepth", 1,
                    "numberOfDecisionTrees", 1,
                    "maxFeaturesRatio", Map.of("range", List.of(0.05, 0.1))
                ),
                TrainingMethod.RandomForestClassification
            ));
        pipeline.addTrainerConfig(
            MLPClassifierTrainConfigImpl.builder().hiddenLayerSizes(List.of(1)).build()
        );

        pipeline.addFeatureStep(new L2FeatureStep(List.of("scalar", "array")));
        return pipeline;
    }

    private LinkPredictionTrainConfig trainingConfig(String modelName) {
        return trainingConfig(modelName, List.of(AUCPR));
    }

    private LinkPredictionTrainConfig trainingConfig(String modelName, List<Metric> metrics) {
        return LinkPredictionTrainConfigImpl.builder()
            .username("DUMMY")
            .modelName(modelName)
            .graphName("g")
            .targetRelationshipType("REL")
            .sourceNodeLabel("N")
            .targetNodeLabel("N")
            .pipeline("DUMMY")
            .metrics(metrics.stream().map(Metric::name).collect(Collectors.toList()))
            .negativeClassWeight(1)
            .randomSeed(1337L)
            .build();
    }

    private LinkPredictionTrainResult runLinkPrediction(
        LinkPredictionTrainConfig trainConfig
    ) {
        var linkPredictionTrain = new LinkPredictionTrain(
            trainGraph,
            trainGraph,
            linkPredictionPipeline(),
            trainConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        return linkPredictionTrain.compute();
    }

    private GraphDimensions graphDimensionsWithSplits(
        GraphDimensions inputDimensions,
        LinkPredictionSplitConfig splitConfig,
        LinkPredictionTrainConfig trainConfig
    ) {
        return splitConfig.expectedGraphDimensions(inputDimensions, trainConfig.targetRelationshipType());
    }
}
