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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.metrics.classification.AllClassMetric;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.AutoTuningConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.ml.pipeline.AutoTuningConfig.MAX_TRIALS;

@GdlExtension
class NodeClassificationTrainTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})";

    static final NodePropertyPredictionSplitConfig SPLIT_CONFIG = NodePropertyPredictionSplitConfigImpl
        .builder()
        .testFraction(0.33)
        .validationFolds(2)
        .build();

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @MethodSource("metricArguments")
    void selectsTheBestModel(ClassificationMetricSpecification metricSpecification) {

        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("a"));
        pipeline.addFeatureStep(NodeFeatureStep.of("b"));

        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfigImpl.builder().penalty(1 * 2.0 / 3.0 * 0.5).maxEpochs(1).build()
        );
        LogisticRegressionTrainConfig expectedWinner = LogisticRegressionTrainConfigImpl
            .builder()
            .penalty(1 * 2.0 / 3.0 * 0.5)
            .maxEpochs(10000)
            .tolerance(1e-5)
            .build();
        pipeline.addTrainerConfig(TrainingMethod.LogisticRegression, expectedWinner);

        // Should NOT be the winning model, so give it bad hyperparams.
        pipeline.setTrainingParameterSpace(
            TrainingMethod.RandomForest,
            List.of(
                TunableTrainerConfig.of(
                    Map.of(
                        "minSplitSize", 2,
                        "maxDepth", 1,
                        "numberOfDecisionTrees", 1,
                        "maxFeaturesRatio", 0.1
                    ),
                    TrainingMethod.RandomForest
                ),
                TunableTrainerConfig.of(
                    Map.of(
                        "minSplitSize", 2,
                        "maxDepth", 1,
                        "numberOfDecisionTrees", 1,
                        "maxFeaturesRatio", Map.of("range", List.of(0.05, 0.1))
                    ),
                    TrainingMethod.RandomForest
                )
            )
        );

        var config = createConfig("model", metricSpecification, 1L);

        var ncTrain = NodeClassificationTrain.create(
            graph,
            pipeline,
            config,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var result = ncTrain.compute();
        var model = result.model();

        var customInfo = model.customInfo();
        List<ModelStats> validationScores = result.trainingStatistics().getValidationStats(metric);

        assertThat(validationScores).hasSize(MAX_TRIALS);

        double model1Score = validationScores.get(0).avg();
        for (int i = 1; i < MAX_TRIALS; i++) {
            assertThat(model1Score)
                .isNotCloseTo(validationScores.get(i).avg(), Percentage.withPercentage(0.2));
        }

        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams.toMap()).isEqualTo(expectedWinner.toMap());
    }

    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldProduceDifferentMetricsForDifferentTrainings(ClassificationMetricSpecification metricSpecification) {
        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        var bananasPipeline = new NodeClassificationTrainingPipeline();
        bananasPipeline.setSplitConfig(SPLIT_CONFIG);

        bananasPipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        var modelCandidates = List.of(
            Map.<String, Object>of("penalty", 0.0625, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.125, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.25, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 0.5, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 1.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 2.0, "maxEpochs", 1000),
            Map.<String, Object>of("penalty", 4.0, "maxEpochs", 1000)
        );

        modelCandidates
            .stream()
            .map(LogisticRegressionTrainConfig::of)
            .forEach(candidate -> bananasPipeline.addTrainerConfig(TrainingMethod.LogisticRegression, candidate));

        var bananasConfig = createConfig("bananasModel", metricSpecification, 1337L);

        var bananasTrain = NodeClassificationTrain.create(
            graph,
            bananasPipeline,
            bananasConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var arrayPipeline = new NodeClassificationTrainingPipeline();
        arrayPipeline.setSplitConfig(SPLIT_CONFIG);

        arrayPipeline.addFeatureStep(NodeFeatureStep.of("arrayProperty"));

        modelCandidates
            .stream()
            .map(LogisticRegressionTrainConfig::of)
            .forEach(candidate -> arrayPipeline.addTrainerConfig(TrainingMethod.LogisticRegression, candidate));


        var arrayPropertyConfig = createConfig(
            "arrayPropertyModel",
            metricSpecification,
            42L
        );
        var arrayPropertyTrain = NodeClassificationTrain.create(
            graph,
            arrayPipeline,
            arrayPropertyConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var bananasModelTrainResult = bananasTrain.compute();
        var bananasModel = bananasModelTrainResult.model();
        var arrayModelTrainResult = arrayPropertyTrain.compute();
        var arrayPropertyModel = arrayModelTrainResult.model();

        assertThat(arrayPropertyModel)
            .usingRecursiveComparison()
            .withFailMessage("The trained models are exactly the same instance!")
            .isNotSameAs(bananasModel);

        assertThat(arrayPropertyModel.data())
            .usingRecursiveComparison()
            .withFailMessage("Should not produce the same trained `data`!")
            .isNotEqualTo(bananasModel.data());

        var bananasCustomInfo = bananasModel.customInfo();
        var bananasValidationScore = bananasCustomInfo.metrics().get(metric);

        var arrayPropertyCustomInfo = arrayPropertyModel.customInfo();
        var arrayPropertyValidationScores = arrayPropertyCustomInfo.metrics().get(metric);

        assertThat(arrayPropertyValidationScores)
            .usingRecursiveComparison()
            .isNotSameAs(bananasValidationScore)
            .isNotEqualTo(bananasValidationScore);
    }

    @Test
    void shouldLogProgress() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.0625 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );
        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.125 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );

        var metrics = ClassificationMetricSpecification.parse("F1(class=1)");
        var config = createConfig("bananasModel", metrics, 42L);

        var progressTask = progressTask(
            pipeline.splitConfig().validationFolds(),
            pipeline.numberOfModelSelectionTrials()
        );
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);

        progressTracker.beginSubTask();
        NodeClassificationTrain.create(graph, pipeline, config, progressTracker, TerminationFlag.RUNNING_TRUE).compute();
        progressTracker.endSubTask();

        assertThat(testLog.getMessages(INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals())
            .containsExactly(
                "MY DUMMY TASK :: Start",
                "MY DUMMY TASK :: Shuffle and split :: Start",
                "MY DUMMY TASK :: Shuffle and split :: Train set size is 10",
                "MY DUMMY TASK :: Shuffle and split :: Test set size is 5",
                "MY DUMMY TASK :: Shuffle and split 100%",
                "MY DUMMY TASK :: Shuffle and split :: Finished",
                "MY DUMMY TASK :: Select best model :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Parameters: {methodName=LogisticRegression, batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=0.0208}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 50%",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 100%",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Main validation metric: 0.8194",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Validation metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Training metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Finished",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Parameters: {methodName=LogisticRegression, batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=0.0416}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 50%",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 100%",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Main validation metric: 0.8194",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Validation metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Training metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Finished",
                "MY DUMMY TASK :: Select best model :: Best trial was Trial 1 with main validation metric 0.8194",
                "MY DUMMY TASK :: Select best model :: Finished",
                "MY DUMMY TASK :: Train best model :: Start",
                "MY DUMMY TASK :: Train best model :: Epoch 1 with loss 0.6578",
                "MY DUMMY TASK :: Train best model :: Epoch 2 with loss 0.6326",
                "MY DUMMY TASK :: Train best model :: Epoch 3 with loss 0.6171",
                "MY DUMMY TASK :: Train best model :: Epoch 4 with loss 0.6110",
                "MY DUMMY TASK :: Train best model :: Epoch 5 with loss 0.6128",
                "MY DUMMY TASK :: Train best model :: converged after 5 epochs. Initial loss: 0.6931, Last loss: 0.6128.",
                "MY DUMMY TASK :: Train best model 100%",
                "MY DUMMY TASK :: Train best model :: Finished",
                "MY DUMMY TASK :: Evaluate on test data :: Start",
                "MY DUMMY TASK :: Evaluate on test data 100%",
                "MY DUMMY TASK :: Evaluate on test data :: Finished",
                "MY DUMMY TASK :: Retrain best model :: Start",
                "MY DUMMY TASK :: Retrain best model :: Epoch 1 with loss 0.6645",
                "MY DUMMY TASK :: Retrain best model :: Epoch 2 with loss 0.6460",
                "MY DUMMY TASK :: Retrain best model :: Epoch 3 with loss 0.6373",
                "MY DUMMY TASK :: Retrain best model :: Epoch 4 with loss 0.6376",
                "MY DUMMY TASK :: Retrain best model :: converged after 4 epochs. Initial loss: 0.6931, Last loss: 0.6376.",
                "MY DUMMY TASK :: Retrain best model 100%",
                "MY DUMMY TASK :: Retrain best model :: Finished",
                "MY DUMMY TASK :: Final model metrics on test set: {F1_class_1=0.7499}",
                "MY DUMMY TASK :: Final model metrics on full train set: {F1_class_1=0.8235}",
                "MY DUMMY TASK :: Finished"
            );
    }

    @Test
    void shouldLogProgressWithRange() {
        int MAX_TRIALS = 2;
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));

        pipeline.addTrainerConfig(
            TunableTrainerConfig.of(
                Map.of("penalty", Map.of("range", List.of(1e-4, 1e4)), "maxEpochs", 100),
                TrainingMethod.LogisticRegression
            )
        );
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(MAX_TRIALS).build());

        var metrics = ClassificationMetricSpecification.parse("F1(class=1)");
        var config = createConfig("bananasModel", metrics, 42L);

        var progressTask = progressTask(pipeline.splitConfig().validationFolds(), MAX_TRIALS);
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);

        progressTracker.beginSubTask();
        NodeClassificationTrain.create(graph, pipeline, config, progressTracker, TerminationFlag.RUNNING_TRUE).compute();
        progressTracker.endSubTask();

        assertThat(testLog.getMessages(INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals())
            .containsExactly(
                "MY DUMMY TASK :: Start",
                "MY DUMMY TASK :: Shuffle and split :: Start",
                "MY DUMMY TASK :: Shuffle and split :: Train set size is 10",
                "MY DUMMY TASK :: Shuffle and split :: Test set size is 5",
                "MY DUMMY TASK :: Shuffle and split 100%",
                "MY DUMMY TASK :: Shuffle and split :: Finished",
                "MY DUMMY TASK :: Select best model :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Parameters: {methodName=LogisticRegression, batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=0.0019}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 50%",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 100%",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Main validation metric: 0.8194",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Validation metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Training metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 1 of 2 :: Finished",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Start",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Parameters: {methodName=LogisticRegression, batchSize=100, minEpochs=1, patience=1, maxEpochs=100, tolerance=0.001, learningRate=0.001, penalty=0.0566}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 50%",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 100%",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Main validation metric: 0.8194",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Validation metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Training metrics: {F1_class_1=0.8194}",
                "MY DUMMY TASK :: Select best model :: Trial 2 of 2 :: Finished",
                "MY DUMMY TASK :: Select best model :: Best trial was Trial 1 with main validation metric 0.8194",
                "MY DUMMY TASK :: Select best model :: Finished",
                "MY DUMMY TASK :: Train best model :: Start",
                "MY DUMMY TASK :: Train best model :: Epoch 1 with loss 0.6578",
                "MY DUMMY TASK :: Train best model :: Epoch 2 with loss 0.6326",
                "MY DUMMY TASK :: Train best model :: Epoch 3 with loss 0.6171",
                "MY DUMMY TASK :: Train best model :: Epoch 4 with loss 0.6110",
                "MY DUMMY TASK :: Train best model :: Epoch 5 with loss 0.6128",
                "MY DUMMY TASK :: Train best model :: converged after 5 epochs. Initial loss: 0.6931, Last loss: 0.6128.",
                "MY DUMMY TASK :: Train best model 100%",
                "MY DUMMY TASK :: Train best model :: Finished",
                "MY DUMMY TASK :: Evaluate on test data :: Start",
                "MY DUMMY TASK :: Evaluate on test data 100%",
                "MY DUMMY TASK :: Evaluate on test data :: Finished",
                "MY DUMMY TASK :: Retrain best model :: Start",
                "MY DUMMY TASK :: Retrain best model :: Epoch 1 with loss 0.6645",
                "MY DUMMY TASK :: Retrain best model :: Epoch 2 with loss 0.6460",
                "MY DUMMY TASK :: Retrain best model :: Epoch 3 with loss 0.6373",
                "MY DUMMY TASK :: Retrain best model :: Epoch 4 with loss 0.6376",
                "MY DUMMY TASK :: Retrain best model :: converged after 4 epochs. Initial loss: 0.6931, Last loss: 0.6376.",
                "MY DUMMY TASK :: Retrain best model 100%",
                "MY DUMMY TASK :: Retrain best model :: Finished",
                "MY DUMMY TASK :: Final model metrics on test set: {F1_class_1=0.7499}",
                "MY DUMMY TASK :: Final model metrics on full train set: {F1_class_1=0.8235}",
                "MY DUMMY TASK :: Finished"
            );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void seededNodeClassification(int concurrency) {
        var pipeline = new NodeClassificationTrainingPipeline();

        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("bananas"));
        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.0625, "maxEpochs", 100, "batchSize", 1))
        );

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .graphName("IGNORE")
            .pipeline("IGNORE")
            .username("IGNORE")
            .modelName("model")
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(ClassificationMetricSpecification.parse("Accuracy")))
            .concurrency(concurrency)
            .build();

        Supplier<NodeClassificationTrain> algoSupplier = () -> NodeClassificationTrain.create(
            graph,
            pipeline,
            config,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var firstResult = algoSupplier.get().compute();
        var secondResult = algoSupplier.get().compute();

        assertThat(((LogisticRegressionData) firstResult.model().data()).weights().data())
            .matches(matrix -> matrix.equals(
                ((LogisticRegressionData) secondResult.model().data()).weights().data(),
                1e-10
            ));
    }

    private static Task progressTask(int validationFolds, int trials) {
        return Tasks.task(
            "MY DUMMY TASK",
            NodeClassificationTrain.progressTasks(validationFolds, trials)
        );
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        ClassificationMetricSpecification metricSpecification,
        long randomSeed
    ) {
        return NodeClassificationPipelineTrainConfigImpl.builder()
            .graphName("IGNORE")
            .pipeline("IGNORE")
            .username("IGNORE")
            .modelName(modelName)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
    }

    static Stream<Arguments> metricArguments() {
        var singleClassMetrics = Stream.of(Arguments.arguments(ClassificationMetricSpecification.parse("F1(class=1)")));
        var allClassMetrics = Arrays
            .stream(AllClassMetric.values())
            .map(AllClassMetric::name)
            .map(ClassificationMetricSpecification::parse)
            .map(Arguments::of);
        return Stream.concat(singleClassMetrics, allClassMetrics);
    }
}
