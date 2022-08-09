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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.MetricConsumer;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetric;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodeClassification.ClassificationMetricComputer;
import org.neo4j.gds.ml.nodePropertyPrediction.NodeSplitter;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.training.CrossValidation;
import org.neo4j.gds.ml.training.TrainingStatistics;
import org.openjdk.jol.util.Multiset;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.LabelsAndClassCountsExtractor.extractLabelsAndClassCounts;
import static org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig.classificationMetrics;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeClassificationTrain {

    private final NodeClassificationPipelineTrainConfig trainConfig;
    private final NodeClassificationTrainingPipeline pipeline;
    private final Features features;
    private final HugeLongArray targets;
    private final LocalIdMap classIdMap;
    private final List<Metric> metrics;
    private final List<ClassificationMetric> classificationMetrics;
    private final Multiset<Long> classCounts;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public static MemoryEstimation estimate(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config
    ) {
        var fudgedClassCount = 1000;
        var fudgedFeatureCount = 500;
        NodePropertyPredictionSplitConfig splitConfig = pipeline.splitConfig();
        var testFraction = splitConfig.testFraction();

        var modelSelection = modelTrainAndEvaluateMemoryUsage(
            pipeline,
            fudgedClassCount,
            fudgedFeatureCount,
            splitConfig::foldTrainSetSize,
            splitConfig::foldTestSetSize
        );
        var bestModelEvaluation = delegateEstimation(
            modelTrainAndEvaluateMemoryUsage(
                pipeline,
                fudgedClassCount,
                fudgedFeatureCount,
                splitConfig::trainSetSize,
                splitConfig::testSetSize
            ),
            "best model evaluation"
        );

        var modelTrainingEstimation = maxEstimation(List.of(modelSelection, bestModelEvaluation));

        // Final step is to retrain the best model with the entire node set.
        // Training memory is independent of node set size so we can skip that last estimation.
        var builder = MemoryEstimations.builder()
            .perNode("global targets", HugeLongArray::memoryEstimation)
            .rangePerNode("global class counts", __ -> MemoryRange.of(2 * Long.BYTES, fudgedClassCount * Long.BYTES))
            .add("metrics", ClassificationMetricSpecification.memoryEstimation(fudgedClassCount))
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .add("outer split", FractionSplitter.estimate(1 - testFraction))
            .add(
                "inner split",
                StratifiedKFoldSplitter.memoryEstimationForNodeSet(splitConfig.validationFolds(), 1 - testFraction)
            )
            .add(
                "stats map train",
                TrainingStatistics.memoryEstimationStatsMap(config.metrics().size(), pipeline.numberOfModelSelectionTrials())
            )
            .add(
                "stats map validation",
                TrainingStatistics.memoryEstimationStatsMap(config.metrics().size(), pipeline.numberOfModelSelectionTrials())
            )
            .add("max of model selection and best model evaluation", modelTrainingEstimation);

        if (!pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestClassification).isEmpty()) {
            // Having a random forest model candidate forces using eager feature extraction.
            builder.perGraphDimension("cached feature vectors", (dim, threads) -> MemoryRange.of(
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(10)),
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(fudgedFeatureCount))
            ));
        }

        return builder.build();
    }

    public static List<Task> progressTasks(NodePropertyPredictionSplitConfig splitConfig, int numberOfModelSelectionTrials, long nodeCount) {
        long trainSetSize = splitConfig.trainSetSize(nodeCount);
        long testSetSize = splitConfig.testSetSize(nodeCount);
        int validationFolds = splitConfig.validationFolds();

        var tasks = new ArrayList<>(CrossValidation.progressTasks(validationFolds, numberOfModelSelectionTrials, trainSetSize));
        tasks.add(ClassifierTrainer.progressTask("Train best model", 5 * trainSetSize));
        tasks.add(Tasks.leaf("Evaluate on train data", trainSetSize));
        tasks.add(Tasks.leaf("Evaluate on test data", testSetSize));
        tasks.add(ClassifierTrainer.progressTask("Retrain best model", 5 * nodeCount));

        return tasks;
    }

    @NotNull
    private static MemoryEstimation modelTrainAndEvaluateMemoryUsage(
        NodeClassificationTrainingPipeline pipeline,
        int fudgedClassCount,
        int fudgedFeatureCount,
        LongUnaryOperator trainSetSize,
        LongUnaryOperator testSetSize
    ) {
        var foldEstimations = pipeline
            .trainingParameterSpace()
            .values()
            .stream()
            .flatMap(List::stream)
            .flatMap(TunableTrainerConfig::streamCornerCaseConfigs)
            .map(
                config ->
                    MemoryEstimations.setup("max of training and evaluation", dim ->
                        {
                            var training = ClassifierTrainerFactory.memoryEstimation(
                                config,
                                trainSetSize,
                                (int) Math.min(fudgedClassCount, dim.nodeCount()),
                                MemoryRange.of(fudgedFeatureCount),
                                false
                            );

                            int batchSize = config instanceof LogisticRegressionTrainConfig
                                ? ((LogisticRegressionTrainConfig) config).batchSize()
                                : 0; // Not used
                            var evaluation = ClassificationMetricComputer.estimateEvaluation(
                                config,
                                (int) Math.min(batchSize, dim.nodeCount()),
                                trainSetSize,
                                testSetSize,
                                (int) Math.min(fudgedClassCount, dim.nodeCount()),
                                fudgedFeatureCount,
                                false
                            );

                            return MemoryEstimations.maxEstimation(List.of(training, evaluation));
                        }
                    ))
            .collect(Collectors.toList());

        return MemoryEstimations.builder("model selection")
            .max(foldEstimations)
            .build();
    }

    public static NodeClassificationTrain create(
        Graph graph,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());
        var labelsAndClassCounts = extractLabelsAndClassCounts(targetNodeProperty, graph.nodeCount());
        Multiset<Long> classCounts = labelsAndClassCounts.classCounts();
        var labels = labelsAndClassCounts.labels();
        var classIdMap = LocalIdMap.ofSorted(classCounts.keys());
        var metrics = config.metrics(classCounts.keys());
        var classificationMetrics = classificationMetrics(metrics);

        Features features;
        if (pipeline.trainingParameterSpace().get(TrainingMethod.RandomForestClassification).isEmpty()) {
            features = FeaturesFactory.extractLazyFeatures(graph, pipeline.featureProperties());
        } else {
            // Random forest uses feature vectors many times each.
            features = FeaturesFactory.extractEagerFeatures(graph, pipeline.featureProperties());
        }

        return new NodeClassificationTrain(
            pipeline,
            config,
            features,
            labels,
            classIdMap,
            metrics,
            classificationMetrics,
            classCounts,
            progressTracker,
            terminationFlag
        );
    }

    private NodeClassificationTrain(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        Features features,
        HugeLongArray labels,
        LocalIdMap classIdMap,
        List<Metric> metrics,
        List<ClassificationMetric> classificationMetrics,
        Multiset<Long> classCounts,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.classificationMetrics = classificationMetrics;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.pipeline = pipeline;
        this.trainConfig = config;
        this.features = features;
        this.targets = labels;
        this.classIdMap = classIdMap;
        this.metrics = metrics;
        this.classCounts = classCounts;
    }

    public NodeClassificationTrainResult compute() {
        var splitConfig = pipeline.splitConfig();
        var nodeSplits = new NodeSplitter(
            features.size(),
            progressTracker
        ).split(
            splitConfig.testFraction(),
            splitConfig.validationFolds(),
            trainConfig.randomSeed()
        );

        var trainingStatistics = new TrainingStatistics(metrics);

        findBestModelCandidate(nodeSplits.outerSplit().trainSet(), trainingStatistics);

        evaluateBestModel(nodeSplits.outerSplit(), trainingStatistics);

        Classifier retrainedModelData = retrainBestModel(nodeSplits.allTrainingExamples(), trainingStatistics.bestParameters());

        return ImmutableNodeClassificationTrainResult.of(retrainedModelData, trainingStatistics);
    }

    private void findBestModelCandidate(ReadOnlyHugeLongArray trainNodeIds, TrainingStatistics trainingStatistics) {
        var crossValidation = new CrossValidation<>(
            progressTracker,
            terminationFlag,
            metrics,
            pipeline.splitConfig().validationFolds(),
            trainConfig.randomSeed(),
            (trainSet, config, metricsHandler, messageLogLevel) -> trainModel(
                trainSet,
                config,
                messageLogLevel,
                metricsHandler
            ),
            (evaluationSet, classifier, scoreConsumer) -> registerMetricScores(
                evaluationSet,
                classifier,
                scoreConsumer,
                ProgressTracker.NULL_TRACKER
            )
        );

        var modelCandidates = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.autoTuningConfig().maxTrials(),
            trainConfig.randomSeed()
        );

        crossValidation.selectModel(
            trainNodeIds,
            targets::get,
            new TreeSet<>(classCounts.keys()),
            trainingStatistics,
            modelCandidates
        );
    }

    private void registerMetricScores(
        ReadOnlyHugeLongArray evaluationSet,
        Classifier classifier,
        MetricConsumer scoreConsumer,
        ProgressTracker customProgressTracker
    ) {
        var trainMetricComputer = ClassificationMetricComputer.forEvaluationSet(
            features,
            targets,
            classCounts,
            evaluationSet,
            classifier,
            trainConfig.concurrency(),
            terminationFlag,
            customProgressTracker
        );
        // currently no specific metrics are evaluated on test
        classificationMetrics.forEach(metric -> scoreConsumer.consume(metric, trainMetricComputer.score(metric)));
    }

    private void evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("Train best model");
        var bestClassifier = trainModel(
            outerSplit.trainSet(),
            trainingStatistics.bestParameters(),
            LogLevel.INFO,
            ModelSpecificMetricsHandler.of(metrics, trainingStatistics::addTestScore)
        );
        progressTracker.endSubTask("Train best model");

        progressTracker.beginSubTask("Evaluate on train data");
        progressTracker.setSteps(outerSplit.trainSet().size());
        registerMetricScores(outerSplit.trainSet(), bestClassifier, trainingStatistics::addOuterTrainScore, progressTracker);
        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));
        progressTracker.endSubTask("Evaluate on train data");

        progressTracker.beginSubTask("Evaluate on test data");
        progressTracker.setSteps(outerSplit.testSet().size());
        registerMetricScores(outerSplit.testSet(), bestClassifier, trainingStatistics::addTestScore, progressTracker);
        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on test set: %s", testMetrics));
        progressTracker.endSubTask("Evaluate on test data");
    }

    private Classifier retrainBestModel(ReadOnlyHugeLongArray trainSet, TrainerConfig bestParameters) {
        progressTracker.beginSubTask("Retrain best model");
        var retrainedClassifier = trainModel(
            trainSet,
            bestParameters,
            LogLevel.INFO,
            ModelSpecificMetricsHandler.NOOP
        );
        progressTracker.endSubTask("Retrain best model");

        return retrainedClassifier;
    }

    private Classifier trainModel(
        ReadOnlyHugeLongArray trainSet,
        TrainerConfig trainerConfig,
        LogLevel messageLogLevel,
        ModelSpecificMetricsHandler metricsHandler
    ) {
        ClassifierTrainer trainer = ClassifierTrainerFactory.create(
            trainerConfig,
            classIdMap,
            terminationFlag,
            progressTracker,
            messageLogLevel,
            trainConfig.concurrency(),
            trainConfig.randomSeed(),
            false,
            metricsHandler
        );

        return trainer.train(features, targets, trainSet);
    }

}
