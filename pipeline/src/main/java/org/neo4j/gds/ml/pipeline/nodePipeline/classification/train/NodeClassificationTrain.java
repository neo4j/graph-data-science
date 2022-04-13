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
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.metrics.StatsMap;
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
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.openjdk.jol.util.Multiset;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.LabelsAndClassCountsExtractor.extractLabelsAndClassCounts;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeClassificationTrain {

    private final Graph graph;
    private final NodeClassificationPipelineTrainConfig config;
    private final NodeClassificationTrainingPipeline pipeline;
    private final Features features;
    private final HugeLongArray targets;
    private final LocalIdMap classIdMap;
    private final List<ClassificationMetric> metrics;
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
                StatsMap.memoryEstimation(config.metrics().size(), pipeline.numberOfModelSelectionTrials())
            )
            .add(
                "stats map validation",
                StatsMap.memoryEstimation(config.metrics().size(), pipeline.numberOfModelSelectionTrials())
            )
            .add("max of model selection and best model evaluation", modelTrainingEstimation);

        if (!pipeline.trainingParameterSpace().get(TrainingMethod.RandomForest).isEmpty()) {
            // Having a random forest model candidate forces using eager feature extraction.
            builder.perGraphDimension("cached feature vectors", (dim, threads) -> MemoryRange.of(
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(10)),
                HugeObjectArray.memoryEstimation(dim.nodeCount(), sizeOfDoubleArray(fudgedFeatureCount))
            ));
        }

        return builder.build();
    }

    public static List<Task> progressTasks(int validationFolds, int numberOfModelSelectionTrials) {
        return List.of(
            Tasks.leaf("Shuffle and split"),
            Tasks.iterativeFixed(
                "Select best model",
                () -> List.of(Tasks.leaf("Trial", validationFolds)),
                numberOfModelSelectionTrials
            ),
            ClassifierTrainer.progressTask("Train best model"),
            Tasks.leaf("Evaluate on test data"),
            ClassifierTrainer.progressTask("Retrain best model")
        );
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
        HugeLongArray labels = labelsAndClassCounts.labels();
        var classIdMap = LocalIdMap.ofSorted(classCounts.keys());
        var metrics = config.metrics(classCounts.keys());

        Features features;
        if (pipeline.trainingParameterSpace().get(TrainingMethod.RandomForest).isEmpty()) {
            features = FeaturesFactory.extractLazyFeatures(graph, pipeline.featureProperties());
        } else {
            // Random forest uses feature vectors many times each.
            features = FeaturesFactory.extractEagerFeatures(graph, pipeline.featureProperties());
        }

        return new NodeClassificationTrain(
            graph,
            pipeline,
            config,
            features,
            labels,
            classIdMap,
            metrics,
            classCounts,
            progressTracker,
            terminationFlag
        );
    }

    private NodeClassificationTrain(
        Graph graph,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        Features features,
        HugeLongArray labels,
        LocalIdMap classIdMap,
        List<ClassificationMetric> metrics,
        Multiset<Long> classCounts,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.graph = graph;
        this.pipeline = pipeline;
        this.config = config;
        this.features = features;
        this.targets = labels;
        this.classIdMap = classIdMap;
        this.metrics = metrics;
        this.classCounts = classCounts;
    }

    public NodeClassificationTrainResult compute() {
        progressTracker.beginSubTask("Shuffle and split");

        var splitConfig = pipeline.splitConfig();
        var nodeSplits = new NodeSplitter(
            features.size(),
            targets::get,
            new TreeSet<>(classCounts.keys()),
            progressTracker
        ).split(
            splitConfig.testFraction(),
            splitConfig.validationFolds(),
            config.randomSeed()
        );

        progressTracker.endSubTask("Shuffle and split");

        var trainingStatistics = new TrainingStatistics(List.copyOf(metrics));

        selectBestModel(nodeSplits.innerSplits(), trainingStatistics);
        evaluateBestModel(nodeSplits.outerSplit(), trainingStatistics);

        Classifier retrainedModelData = retrainBestModel(nodeSplits.allTrainingExamples(), trainingStatistics.bestParameters());

        var testMetrics = trainingStatistics.metricsForWinningModel().entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().test()));
        var outerTrainMetrics = trainingStatistics.metricsForWinningModel().entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().outerTrain()));
        progressTracker.logMessage(formatWithLocale("Final model metrics on test set: %s", testMetrics));
        progressTracker.logMessage(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));

        return ImmutableNodeClassificationTrainResult.of(
            createModel(retrainedModelData, trainingStatistics),
            trainingStatistics
        );
    }

    private void selectBestModel(List<TrainingExamplesSplit> nodeSplits, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask("Select best model");

        var hyperParameterOptimizer = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.numberOfModelSelectionTrials(),
            config.randomSeed()
        );


        int currentTrial = 1;
        int bestTrial = -1;
        double bestTrialScore = -1e42;

        while (hyperParameterOptimizer.hasNext()) {
            progressTracker.beginSubTask("Trial");
            var modelParams = hyperParameterOptimizer.next();
            progressTracker.logMessage(formatWithLocale("Parameters: %s", modelParams.toMap()));
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());

            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();

                var classifier = trainModel(trainSet, modelParams, ProgressTracker.NULL_TRACKER);

                registerMetricScores(validationSet, classifier, validationStatsBuilder::update);
                registerMetricScores(trainSet, classifier, trainStatsBuilder::update);
                progressTracker.logProgress();
            }

            metrics.forEach(metric -> {
                trainingStatistics.addValidationStats(metric, validationStatsBuilder.build(metric));
                trainingStatistics.addTrainStats(metric, trainStatsBuilder.build(metric));
            });
            var validationStats = trainingStatistics.findModelValidationAvg(modelParams);
            var trainStats = trainingStatistics.findModelTrainAvg(modelParams);
            double mainMetric = validationStats.get(metrics.get(0));
            if (mainMetric > bestTrialScore) {
                bestTrial = currentTrial;
                bestTrialScore = mainMetric;
            }

            progressTracker.logMessage(formatWithLocale("Main validation metric: %.4f", mainMetric));
            progressTracker.logMessage(formatWithLocale("Validation metrics: %s", validationStats));
            progressTracker.logMessage(formatWithLocale("Training metrics: %s", trainStats));

            currentTrial++;

            progressTracker.endSubTask("Trial");
        }
        progressTracker.logMessage(formatWithLocale(
            "Best trial was Trial %d with main validation metric %.4f",
            bestTrial,
            bestTrialScore
        ));
        progressTracker.endSubTask("Select best model");
    }

    private void registerMetricScores(
        HugeLongArray evaluationSet,
        Classifier classifier,
        BiConsumer<Metric, Double> scoreConsumer
    ) {
        var trainMetricComputer = ClassificationMetricComputer.forEvaluationSet(
            features,
            targets,
            classCounts,
            evaluationSet,
            classifier,
            config.concurrency(),
            terminationFlag
        );
        metrics.forEach(metric -> scoreConsumer.accept(metric, trainMetricComputer.score(metric)));
    }

    private void evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("Train best model");
        var bestClassifier = trainModel(outerSplit.trainSet(), trainingStatistics.bestParameters(), progressTracker);
        progressTracker.endSubTask("Train best model");

        progressTracker.beginSubTask("Evaluate on test data", outerSplit.testSet().size() + outerSplit.trainSet().size());
        registerMetricScores(outerSplit.testSet(), bestClassifier, trainingStatistics::addTestScore);
        registerMetricScores(outerSplit.trainSet(), bestClassifier, trainingStatistics::addOuterTrainScore);
        progressTracker.endSubTask("Evaluate on test data");
    }

    private Classifier retrainBestModel(HugeLongArray trainSet, TrainerConfig bestParameters) {
        progressTracker.beginSubTask("Retrain best model");
        var retrainedClassifier = trainModel(trainSet, bestParameters, progressTracker);
        progressTracker.endSubTask("Retrain best model");

        return retrainedClassifier;
    }

    private Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> createModel(
        Classifier classifier,
        TrainingStatistics trainingStatistics
    ) {

        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(classIdMap.originalIdsList())
            .bestParameters(trainingStatistics.bestParameters())
            .metrics(trainingStatistics.metricsForWinningModel())
            .pipeline(NodeClassificationPredictPipeline.from(pipeline))
            .build();

        return Model.of(
            config.username(),
            config.modelName(),
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            graph.schema(),
            classifier.data(),
            config,
            modelInfo
        );
    }

    private Classifier trainModel(
        HugeLongArray trainSet,
        TrainerConfig trainerConfig,
        ProgressTracker customProgressTracker
    ) {
        ClassifierTrainer trainer = ClassifierTrainerFactory.create(
            trainerConfig,
            classIdMap,
            terminationFlag,
            customProgressTracker,
            config.concurrency(),
            config.randomSeed(),
            false
        );

        return trainer.train(features, targets, ReadOnlyHugeLongArray.of(trainSet));
    }

}
