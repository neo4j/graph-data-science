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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.mem.MemoryEstimation;
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
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.nodeClassification.ClassificationMetricComputer;
import org.neo4j.gds.ml.nodePropertyPrediction.NodeSplitter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineTrainer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.training.CrossValidation;
import org.neo4j.gds.ml.training.TrainingStatistics;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
        ModelCatalog modelCatalog,
        AlgorithmsProcedureFacade algorithmsProcedureFacade
    ) {
        return new NodeClassificationTrainMemoryEstimateDefinition(
            pipeline,
            configuration,
            modelCatalog,
            algorithmsProcedureFacade
        ).memoryEstimation();
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

    public static NodeClassificationTrain create(
        GraphStore graphStore,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        NodeFeatureProducer<NodeClassificationPipelineTrainConfig> nodeFeatureProducer,
        ProgressTracker progressTracker
    ) {
        // we dont resolve the relationships as for extracting the classes they are irrelevant
        var nodesGraph = graphStore.getGraph(config.targetNodeLabelIdentifiers(graphStore));
        pipeline.splitConfig().validateMinNumNodesInSplitSets(nodesGraph);

        var targetNodeProperty = nodesGraph.nodeProperties(config.targetProperty());
        var labelsAndClassCounts = extractLabelsAndClassCounts(targetNodeProperty, nodesGraph.nodeCount());
        LongMultiSet classCounts = labelsAndClassCounts.classCounts();
        var classIdMap = LocalIdMap.ofSorted(classCounts.keys());

        var metrics = config.metrics(classIdMap, classCounts);
        return new NodeClassificationTrain(
            pipeline,
            config,
            labelsAndClassCounts.labels(),
            classIdMap,
            nodesGraph,
            metrics,
            classificationMetrics(metrics),
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
            trainConfig.typedConcurrency(),
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

        var sortedClassIds = LongStream
            .range(0, classCounts.size())
            .boxed()
            .collect(Collectors.toCollection(TreeSet::new));

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
            trainConfig.typedConcurrency(),
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
            trainConfig.typedConcurrency(),
            trainConfig.randomSeed(),
            false,
            metricsHandler
        );

        return trainer.train(features, targets, trainSet);
    }
}
