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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.metrics.MetricConsumer;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.RegressionTrainerFactory;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.nodePropertyPrediction.NodeSplitter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.PipelineTrainer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyTrainingPipeline;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.training.CrossValidation;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeRegressionTrain implements PipelineTrainer<NodeRegressionTrainResult> {

    private final HugeDoubleArray targets;
    private final IdMap nodeIdMap;
    private final NodeRegressionTrainingPipeline pipeline;

    private final List<RegressionMetrics> metrics;
    private final NodeRegressionPipelineTrainConfig trainConfig;
    private final NodeFeatureProducer<NodeRegressionPipelineTrainConfig> nodeFeatureProducer;
    private final ProgressTracker progressTracker;
    private TerminationFlag terminationFlag = TerminationFlag.RUNNING_TRUE;

    public static Task progressTask(
        NodePropertyTrainingPipeline pipeline,
        long nodeCount
    ) {
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
        tasks.add(Tasks.leaf("Evaluate on test data", testSetSize));
        tasks.add(ClassifierTrainer.progressTask("Retrain best model", 5 * nodeCount));

        return Tasks.task("Node Regression Train Pipeline", tasks);
    }

    private static HugeDoubleArray createTargets(Graph graph, String targetProperty) {
        var targetNodeProperty = graph.nodeProperties(targetProperty);
        HugeDoubleArray targets = HugeDoubleArray.newArray(graph.nodeCount());

        for (long i = 0; i < graph.nodeCount(); i++) {
            double value = targetNodeProperty.doubleValue(i);
            if (Double.isNaN(value)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node with id %d has `%s` target property value `NaN`",
                    graph.toOriginalNodeId(i),
                    targetProperty
                ));
            }
            if (Double.isInfinite(value)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node with id %d has infinite `%s` target property value",
                    graph.toOriginalNodeId(i),
                    targetProperty
                ));
            }
            targets.set(i, value);
        }

        return targets;
    }

    public static NodeRegressionTrain create(
        GraphStore graphStore,
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig config,
        NodeFeatureProducer<NodeRegressionPipelineTrainConfig> nodeFeatureProducer,
        ProgressTracker progressTracker
    ) {

        var nodesGraph = graphStore.getGraph(config.targetNodeLabelIdentifiers(graphStore));
        pipeline.splitConfig().validateMinNumNodesInSplitSets(nodesGraph);

        return new NodeRegressionTrain(
            pipeline,
            config,
            nodeFeatureProducer,
            createTargets(nodesGraph, config.targetProperty()),
            nodesGraph,
            config.metrics(),
            progressTracker
        );
    }

    private NodeRegressionTrain(
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig trainConfig,
        NodeFeatureProducer<NodeRegressionPipelineTrainConfig> nodeFeatureProducer,
        HugeDoubleArray targets,
        IdMap nodeIdMap,
        List<RegressionMetrics> metrics,
        ProgressTracker progressTracker
    ) {
        this.pipeline = pipeline;
        this.trainConfig = trainConfig;
        this.nodeFeatureProducer = nodeFeatureProducer;
        this.nodeIdMap = nodeIdMap;
        this.metrics = metrics;
        this.progressTracker = progressTracker;
        this.targets = targets;
    }

    @Override
    public void setTerminationFlag(TerminationFlag terminationFlag) {
        this.terminationFlag = terminationFlag;
    }

    @Override
    public NodeRegressionTrainResult run() {
        progressTracker.beginSubTask();
        var splitConfig = pipeline.splitConfig();
        var splits = new NodeSplitter(
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

        terminationFlag.assertRunning();

        var trainingStatistics = new TrainingStatistics(metrics);

        var features = nodeFeatureProducer.procedureFeatures(pipeline);

        findBestModelCandidate(splits.outerSplit().trainSet(), metrics, features, trainingStatistics);

        evaluateBestModel(splits.outerSplit(), features, trainingStatistics);

        var retrainedModel = retrainBestModel(
            splits.allTrainingExamples(),
            features,
            trainingStatistics.bestParameters()
        );

        progressTracker.endSubTask();
        return ImmutableNodeRegressionTrainResult.of(retrainedModel, trainingStatistics);
    }

    private void findBestModelCandidate(
        ReadOnlyHugeLongArray trainNodeIds,
        List<RegressionMetrics> metrics,
        Features features,
        TrainingStatistics trainingStatistics
    ) {
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
                messageLogLevel
            ),
            (evaluationSet, regressor, scoreConsumer) -> registerMetricScores(
                evaluationSet,
                regressor,
                features,
                scoreConsumer
            )

        );

        var modelCandidates = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.autoTuningConfig().maxTrials(),
            trainConfig.randomSeed()
        );

        crossValidation.selectModel(
            trainNodeIds,
            id -> 0L,
            new TreeSet<>(List.of(0L)),
            trainingStatistics,
            modelCandidates
        );
    }

    private void registerMetricScores(
        ReadOnlyHugeLongArray evaluationSet,
        Regressor regressor,
        Features features,
        MetricConsumer scoreConsumer
    ) {
        var localPredictions = HugeDoubleArray.newArray(evaluationSet.size());
        ParallelUtil.parallelForEachNode(
            evaluationSet.size(),
            trainConfig.concurrency(),
            idx -> localPredictions.set(idx, regressor.predict(features.get(evaluationSet.get(idx))))
        );

        terminationFlag.assertRunning();

        HugeDoubleArray localTargets = HugeDoubleArray.newArray(evaluationSet.size());
        ParallelUtil.parallelForEachNode(
            evaluationSet.size(),
            trainConfig.concurrency(),
            idx -> localTargets.set(idx, targets.get(evaluationSet.get(idx)))
        );

        metrics.forEach(metric -> scoreConsumer.consume(metric, metric.compute(localTargets, localPredictions)));
    }

    private void evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        Features features,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("Train best model");
        var bestRegressor = trainModel(
            outerSplit.trainSet(),
            trainingStatistics.bestParameters(),
            features,
            LogLevel.INFO
        );
        progressTracker.endSubTask("Train best model");

        progressTracker.beginSubTask("Evaluate on test data");

        registerMetricScores(outerSplit.trainSet(), bestRegressor, features, trainingStatistics::addOuterTrainScore);
        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));

        registerMetricScores(outerSplit.testSet(), bestRegressor, features, trainingStatistics::addTestScore);
        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on test set: %s", testMetrics));

        progressTracker.endSubTask("Evaluate on test data");
    }

    private Regressor retrainBestModel(
        ReadOnlyHugeLongArray trainSet,
        Features features,
        TrainerConfig bestParameters
    ) {
        progressTracker.beginSubTask("Retrain best model");
        var retrainedRegressor = trainModel(trainSet, bestParameters, features, LogLevel.INFO);
        progressTracker.endSubTask("Retrain best model");

        return retrainedRegressor;
    }

    private Regressor trainModel(
        ReadOnlyHugeLongArray trainSet,
        TrainerConfig trainerConfig,
        Features features,
        LogLevel messageLogLevel
    ) {
        var trainer = RegressionTrainerFactory.create(
            trainerConfig,
            terminationFlag,
            progressTracker,
            messageLogLevel,
            trainConfig.concurrency(),
            trainConfig.randomSeed()
        );

        return trainer.train(features, targets, trainSet);
    }

}
