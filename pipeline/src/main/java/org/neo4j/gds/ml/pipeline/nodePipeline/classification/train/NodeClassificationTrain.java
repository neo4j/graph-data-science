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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
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
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetric;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodeClassification.ClassificationMetricComputer;
import org.neo4j.gds.ml.nodePropertyPrediction.NodeSplitter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineTrainer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.training.CrossValidation;
import org.neo4j.gds.ml.training.TrainingStatistics;

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

public final class NodeClassificationTrain implements PipelineTrainer<NodeClassificationTrainResult> {

    private final NodeClassificationTrainingPipeline pipeline;
    private final NodeClassificationPipelineTrainConfig trainConfig;
    private final HugeIntArray targets;
    private final LocalIdMap classIdMap;
    private final IdMap nodeIdMap;
    private final List<Metric> metrics;
    private final List<ClassificationMetric> classificationMetrics;
    private final LongMultiSet classCounts;
    private final NodeFeatureProducer<NodeClassificationPipelineTrainConfig> nodeFeatureProducer;
    private final ProgressTracker progressTracker;
    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public static MemoryEstimation estimate(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig configuration,
        ModelCatalog modelCatalog
    ) {
        pipeline.validateTrainingParameterSpace();

        MemoryEstimation nodePropertyStepsEstimation = NodePropertyStepExecutor.estimateNodePropertySteps(
            modelCatalog,
            pipeline.nodePropertySteps(),
            configuration.nodeLabels(),
            configuration.relationshipTypes()
        );

        var trainingEstimation = MemoryEstimations
            .builder()
            .add("Training", estimateExcludingNodePropertySteps(pipeline, configuration))
            .build();

        return maxEstimation(
            "Node Classification Train Pipeline",
            List.of(nodePropertyStepsEstimation, trainingEstimation)
        );
    }

    private static MemoryEstimation estimateExcludingNodePropertySteps(
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
            .perNode("global targets", HugeIntArray::memoryEstimation)
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

    public static Task progressTask(NodeClassificationTrainingPipeline pipeline, long nodeCount) {
        var splitConfig = pipeline.splitConfig();
        long trainSetSize = splitConfig.trainSetSize(nodeCount);
        long testSetSize = splitConfig.testSetSize(nodeCount);
        int validationFolds = splitConfig.validationFolds();

        var tasks = new ArrayList<Task>();
        tasks.add(NodePropertyStepExecutor.tasks(pipeline.nodePropertySteps(), nodeCount));
        tasks.addAll(CrossValidation.progressTasks(
            validationFolds,
            pipeline.numberOfModelSelectionTrials(),
            trainSetSize
        ));
        tasks.add(ClassifierTrainer.progressTask("Train best model", 5 * trainSetSize));
        tasks.add(Tasks.leaf("Evaluate on train data", trainSetSize));
        tasks.add(Tasks.leaf("Evaluate on test data", testSetSize));
        tasks.add(ClassifierTrainer.progressTask("Retrain best model", 5 * nodeCount));

        return Tasks.task("Node Classification Train Pipeline", tasks);
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
        GraphStore graphStore,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        NodeFeatureProducer<NodeClassificationPipelineTrainConfig> nodeFeatureProducer,
        ProgressTracker progressTracker
    ) {
        // we dont resolve the relationships as for extracting the classes they are irrelevant
        var nodesGraph = graphStore.getGraph(config.nodeLabelIdentifiers(graphStore));
        pipeline.splitConfig().validateMinNumNodesInSplitSets(nodesGraph);

        var targetNodeProperty = nodesGraph.nodeProperties(config.targetProperty());
        var labelsAndClassCounts = extractLabelsAndClassCounts(targetNodeProperty, nodesGraph.nodeCount());
        LongMultiSet classCounts = labelsAndClassCounts.classCounts();
        var classIdMap = LocalIdMap.ofSorted(classCounts.keys());

        return new NodeClassificationTrain(
            pipeline,
            config,
            labelsAndClassCounts.labels(),
            classIdMap,
            nodesGraph,
            config.metrics(classIdMap, classCounts),
            classificationMetrics(config.metrics(classIdMap, classCounts)),
            classCounts,
            nodeFeatureProducer,
            progressTracker
        );
    }

    private NodeClassificationTrain(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        HugeIntArray labels,
        LocalIdMap classIdMap,
        IdMap nodeIdMap,
        List<Metric> metrics,
        List<ClassificationMetric> classificationMetrics,
        LongMultiSet classCounts,
        NodeFeatureProducer<NodeClassificationPipelineTrainConfig> nodeFeatureProducer,
        ProgressTracker progressTracker
    ) {
        this.pipeline = pipeline;
        this.nodeIdMap = nodeIdMap;
        this.classificationMetrics = classificationMetrics;
        this.nodeFeatureProducer = nodeFeatureProducer;
        this.trainConfig = config;
        this.targets = labels;
        this.classIdMap = classIdMap;
        this.metrics = metrics;
        this.classCounts = classCounts;
        this.progressTracker = progressTracker;
    }

    @Override
    public void setTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    @Override
    public NodeClassificationTrainResult run() {
        progressTracker.beginSubTask();
        var splitConfig = pipeline.splitConfig();
        var nodeSplits = new NodeSplitter(
            trainConfig.concurrency(),
            nodeIdMap.nodeCount(),
            progressTracker,
            nodeIdMap::toOriginalNodeId,
            nodeIdMap::toMappedNodeId
        ).split(
            splitConfig.testFraction(),
            splitConfig.validationFolds(),
            trainConfig.randomSeed()
        );

        var trainingStatistics = new TrainingStatistics(metrics);

        var features = nodeFeatureProducer.procedureFeatures(pipeline);

        findBestModelCandidate(nodeSplits.outerSplit().trainSet(), features, trainingStatistics);

        evaluateBestModel(nodeSplits.outerSplit(), features, trainingStatistics);

        Classifier retrainedModelData = retrainBestModel(nodeSplits.allTrainingExamples(), features, trainingStatistics.bestParameters());
        progressTracker.endSubTask();

        return ImmutableNodeClassificationTrainResult.of(
            retrainedModelData,
            trainingStatistics,
            classIdMap,
            classCounts
        );
    }

    private void findBestModelCandidate(ReadOnlyHugeLongArray trainNodeIds, Features features, TrainingStatistics trainingStatistics) {
        var crossValidation = new CrossValidation<>(
            progressTracker,
            terminationFlag,
            metrics,
            pipeline.splitConfig().validationFolds(),
            trainConfig.randomSeed(),
            (trainSet, config, metricsHandler, messageLogLevel) -> trainModel(
                trainSet,
                config,
                features,
                messageLogLevel,
                metricsHandler
            ),
            (evaluationSet, classifier, scoreConsumer) -> registerMetricScores(
                evaluationSet,
                classifier,
                features,
                scoreConsumer,
                ProgressTracker.NULL_TRACKER
            )
        );

        var modelCandidates = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.autoTuningConfig().maxTrials(),
            trainConfig.randomSeed()
        );

        var sortedClassIds = new TreeSet<Long>();
        for (long clazz : classCounts.keys()) {
            sortedClassIds.add(clazz);
        }

        crossValidation.selectModel(
            trainNodeIds,
            targets::get,
            sortedClassIds,
            trainingStatistics,
            modelCandidates
        );
    }

    private void registerMetricScores(
        ReadOnlyHugeLongArray evaluationSet,
        Classifier classifier,
        Features features,
        MetricConsumer scoreConsumer,
        ProgressTracker customProgressTracker
    ) {
        var trainMetricComputer = ClassificationMetricComputer.forEvaluationSet(
            features,
            targets,
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
        Features features,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("Train best model");
        ModelCandidateStats bestCandidate = trainingStatistics.bestCandidate();
        var bestClassifier = trainModel(
            outerSplit.trainSet(),
            bestCandidate.trainerConfig(),
            features,
            LogLevel.INFO,
            ModelSpecificMetricsHandler.of(metrics, trainingStatistics::addTestScore)
        );
        progressTracker.endSubTask("Train best model");

        progressTracker.beginSubTask("Evaluate on train data");
        progressTracker.setSteps(outerSplit.trainSet().size());
        registerMetricScores(outerSplit.trainSet(), bestClassifier, features, trainingStatistics::addOuterTrainScore, progressTracker);
        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));
        progressTracker.endSubTask("Evaluate on train data");

        progressTracker.beginSubTask("Evaluate on test data");
        progressTracker.setSteps(outerSplit.testSet().size());
        registerMetricScores(outerSplit.testSet(), bestClassifier, features, trainingStatistics::addTestScore, progressTracker);
        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on test set: %s", testMetrics));
        progressTracker.endSubTask("Evaluate on test data");
    }

    private Classifier retrainBestModel(ReadOnlyHugeLongArray trainSet, Features features, TrainerConfig bestParameters) {
        progressTracker.beginSubTask("Retrain best model");
        var retrainedClassifier = trainModel(
            trainSet,
            bestParameters,
            features,
            LogLevel.INFO,
            ModelSpecificMetricsHandler.NOOP
        );
        progressTracker.endSubTask("Retrain best model");

        return retrainedClassifier;
    }

    private Classifier trainModel(
        ReadOnlyHugeLongArray trainSet,
        TrainerConfig trainerConfig,
        Features features,
        LogLevel messageLogLevel,
        ModelSpecificMetricsHandler metricsHandler
    ) {
        ClassifierTrainer trainer = ClassifierTrainerFactory.create(
            trainerConfig,
            classIdMap.size(),
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
