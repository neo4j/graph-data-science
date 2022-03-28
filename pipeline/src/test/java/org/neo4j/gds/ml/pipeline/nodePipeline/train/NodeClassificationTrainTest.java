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
package org.neo4j.gds.ml.pipeline.nodePipeline.train;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.metrics.AllClassMetric;
import org.neo4j.gds.ml.metrics.MetricSpecification;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationSplitConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationTrainingPipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class NodeClassificationTrainTest {

    // TODO validation
    // at least one config

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

    static final NodeClassificationSplitConfig SPLIT_CONFIG = NodeClassificationSplitConfigImpl
        .builder()
        .testFraction(0.33)
        .validationFolds(2)
        .build();

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @MethodSource("metricArguments")
    void selectsTheBestModel(MetricSpecification metricSpecification) {

        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("a"));
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("b"));

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
        pipeline.addTrainerConfig(TrainingMethod.RandomForest, RandomForestTrainConfigImpl.builder()
            .minSplitSize(2)
            .maxDepth(1)
            .numberOfDecisionTrees(1)
            .maxFeaturesRatio(0.1)
            .build()
        );

        var config = createConfig("model", metricSpecification, 1L);

        var ncTrain = NodeClassificationTrain.create(
            graph,
            pipeline,
            config,
            ProgressTracker.NULL_TRACKER
        );

        var result = ncTrain.compute();
        var model = result.model();

        var customInfo = model.customInfo();
        List<ModelStats> validationScores = result.modelSelectionStatistics().validationStats().get(metric);

        assertThat(validationScores).hasSize(3);

        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        double model3Score = validationScores.get(2).avg();
        assertThat(model1Score)
            .isNotCloseTo(model2Score, Percentage.withPercentage(0.2))
            .isNotCloseTo(model3Score, Percentage.withPercentage(0.2));

        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams).isEqualTo(expectedWinner);
    }

    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldProduceDifferentMetricsForDifferentTrainings(MetricSpecification metricSpecification) {
        var metric = metricSpecification.createMetrics(List.of()).findFirst().get();

        var bananasPipeline = new NodeClassificationTrainingPipeline();
        bananasPipeline.setSplitConfig(SPLIT_CONFIG);

        bananasPipeline.addFeatureStep(NodeClassificationFeatureStep.of("bananas"));

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
            ProgressTracker.NULL_TRACKER
        );

        var arrayPipeline = new NodeClassificationTrainingPipeline();
        arrayPipeline.setSplitConfig(SPLIT_CONFIG);

        arrayPipeline.addFeatureStep(NodeClassificationFeatureStep.of("arrayProperty"));

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
            ProgressTracker.NULL_TRACKER
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

    @ParameterizedTest
    @MethodSource("metricArguments")
    void shouldLogProgress(MetricSpecification metricSpecification) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("bananas"));

        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.0625 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );
        pipeline.addTrainerConfig(
            TrainingMethod.LogisticRegression,
            LogisticRegressionTrainConfig.of(Map.of("penalty", 0.125 * 2.0 / 3.0 * 0.5, "maxEpochs", 100))
        );

        var config = createConfig("bananasModel", metricSpecification, 42L);

        var progressTask = NodeClassificationTrain.progressTask(pipeline.splitConfig().validationFolds(), pipeline.numberOfModelCandidates());
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, testLog, 1, EmptyTaskRegistryFactory.INSTANCE);
        NodeClassificationTrain.create(graph, pipeline, config, progressTracker).compute();

        assertThat(testLog.getMessages(INFO))
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals())
            .containsExactly(
                "NCTrain :: Start",
                "NCTrain :: ShuffleAndSplit :: Start",
                "NCTrain :: ShuffleAndSplit :: Train set size is 10",
                "NCTrain :: ShuffleAndSplit :: Test set size is 5",
                "NCTrain :: ShuffleAndSplit 100%",
                "NCTrain :: ShuffleAndSplit :: Finished",
                "NCTrain :: SelectBestModel :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 1 with loss 0.637639074159",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 2 with loss 0.592215665620",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 3 with loss 0.556577259895",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 4 with loss 0.530247740563",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 5 with loss 0.512608592120",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 6 with loss 0.502939511082",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 7 with loss 0.500436536496",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Epoch 8 with loss 0.503167809769",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: converged after 8 epochs. Initial loss: 0.693147180559, Last loss: 0.503167809769.",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 1 with loss 0.678039074072",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 2 with loss 0.673012155738",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Epoch 3 with loss 0.675865964351",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: converged after 3 epochs. Initial loss: 0.693147180559, Last loss: 0.675865964351.",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Split 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 1 with loss 0.637639115826",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 2 with loss 0.592215832287",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 3 with loss 0.556577634895",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 4 with loss 0.530248407229",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 5 with loss 0.512609633786",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 6 with loss 0.502941011082",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 7 with loss 0.500438555777",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Epoch 8 with loss 0.503170225499",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: converged after 8 epochs. Initial loss: 0.693147180559, Last loss: 0.503170225499.",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 1 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 1 with loss 0.678039115739",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 2 with loss 0.673012322394",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Epoch 3 with loss 0.675866228402",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: converged after 3 epochs. Initial loss: 0.693147180559, Last loss: 0.675866228402.",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Training :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate :: Start",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate 50%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate 100%",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Evaluate :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Split 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Model Candidate 2 of 2 :: Finished",
                "NCTrain :: SelectBestModel :: Finished",
                "NCTrain :: TrainSelectedOnRemainder :: Start",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 1 with loss 0.657839074117",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 2 with loss 0.632615629710",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 3 with loss 0.617174301953",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 4 with loss 0.611031387815",
                "NCTrain :: TrainSelectedOnRemainder :: Epoch 5 with loss 0.612866775693",
                "NCTrain :: TrainSelectedOnRemainder :: converged after 5 epochs. Initial loss: 0.693147180559, Last loss: 0.612866775693.",
                "NCTrain :: TrainSelectedOnRemainder 100%",
                "NCTrain :: TrainSelectedOnRemainder :: Finished",
                "NCTrain :: EvaluateSelectedModel :: Start",
                "NCTrain :: EvaluateSelectedModel 33%",
                "NCTrain :: EvaluateSelectedModel 100%",
                "NCTrain :: EvaluateSelectedModel :: Finished",
                "NCTrain :: RetrainSelectedModel :: Start",
                "NCTrain :: RetrainSelectedModel :: Epoch 1 with loss 0.664572407436",
                "NCTrain :: RetrainSelectedModel :: Epoch 2 with loss 0.646082052793",
                "NCTrain :: RetrainSelectedModel :: Epoch 3 with loss 0.637370639999",
                "NCTrain :: RetrainSelectedModel :: Epoch 4 with loss 0.637608261583",
                "NCTrain :: RetrainSelectedModel :: converged after 4 epochs. Initial loss: 0.693147180559, Last loss: 0.637608261583.",
                "NCTrain :: RetrainSelectedModel 100%",
                "NCTrain :: RetrainSelectedModel :: Finished",
                "NCTrain :: Finished"
            );
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void seededNodeClassification(int concurrency) {
        var pipeline = new NodeClassificationTrainingPipeline();

        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeClassificationFeatureStep.of("bananas"));
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
            .metrics(List.of(MetricSpecification.parse("Accuracy")))
            .concurrency(concurrency)
            .build();

        Supplier<NodeClassificationTrain> algoSupplier = () -> NodeClassificationTrain.create(
            graph,
            pipeline,
            config,
            ProgressTracker.NULL_TRACKER
        );

        var firstResult = algoSupplier.get().compute();
        var secondResult = algoSupplier.get().compute();

        assertThat(((LogisticRegressionData)firstResult.model().data()).weights().data())
            .matches(matrix -> matrix.equals(((LogisticRegressionData)secondResult.model().data()).weights().data(), 1e-10));
    }

    private NodeClassificationPipelineTrainConfig createConfig(
        String modelName,
        MetricSpecification metricSpecification,
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
        var singleClassMetrics = Stream.of(Arguments.arguments(MetricSpecification.parse("F1(class=1)")));
        var allClassMetrics = Arrays
            .stream(AllClassMetric.values())
            .map(AllClassMetric::name)
            .map(MetricSpecification::parse)
            .map(Arguments::of);
        return Stream.concat(singleClassMetrics, allClassMetrics);
    }
}
