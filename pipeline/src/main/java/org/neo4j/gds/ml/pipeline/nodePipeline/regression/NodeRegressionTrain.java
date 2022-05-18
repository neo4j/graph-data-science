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
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.RegressionTrainerFactory;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.nodePropertyPrediction.NodeSplitter;
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;

import java.util.List;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeRegressionTrain {

    private final Features features;
    private final HugeDoubleArray targets;
    private final NodeRegressionTrainingPipeline pipeline;
    private final NodeRegressionPipelineTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public static List<Task> progressTasks(
        NodePropertyPredictionSplitConfig splitConfig,
        int numberOfModelSelectionTrials,
        long nodeCount
    ) {
        long trainSetSize = splitConfig.trainSetSize(nodeCount);
        long testSetSize = splitConfig.testSetSize(nodeCount);
        int validationFolds = splitConfig.validationFolds();

        return List.of(
            Tasks.leaf("Shuffle and Split", validationFolds * trainSetSize + testSetSize),
            Tasks.iterativeFixed(
                "Select best model",
                () -> List.of(Tasks.leaf("Trial", 5 * validationFolds * trainSetSize)),
                numberOfModelSelectionTrials
            ),
            ClassifierTrainer.progressTask("Train best model", 5 * trainSetSize),
            Tasks.leaf("Evaluate on test data", testSetSize),
            ClassifierTrainer.progressTask("Retrain best model", 5 * nodeCount)
        );
    }

    public static NodeRegressionTrain create(
        Graph graph,
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig trainConfig,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var targetNodeProperty = graph.nodeProperties(trainConfig.targetProperty());

        HugeDoubleArray targets = HugeDoubleArray.newArray(graph.nodeCount());
        targets.setAll(targetNodeProperty::doubleValue);

        Features features;
        if (pipeline.trainingParameterSpace().getOrDefault(TrainingMethod.RandomForestRegression, List.of()).isEmpty()) {
            features = FeaturesFactory.extractLazyFeatures(graph, pipeline.featureProperties());
        } else {
            // Random forest uses feature vectors many times each.
            features = FeaturesFactory.extractEagerFeatures(graph, pipeline.featureProperties());
        }

        return new NodeRegressionTrain(
            pipeline,
            trainConfig,
            features,
            targets,
            progressTracker,
            terminationFlag
        );
    }

    NodeRegressionTrain(
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig trainConfig,
        Features features,
        HugeDoubleArray targets,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.pipeline = pipeline;
        this.trainConfig = trainConfig;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.features = features;
        this.targets = targets;
    }

    public NodeRegressionTrainResult compute() {
        progressTracker.beginSubTask("Shuffle and Split");

        var splitConfig = pipeline.splitConfig();
        var splits = new NodeSplitter(
            features.size(),
            id -> 0L,
            new TreeSet<>(List.of(0L)),
            progressTracker
        ).split(
            splitConfig.testFraction(),
            splitConfig.validationFolds(),
            trainConfig.randomSeed()
        );

        terminationFlag.assertRunning();
        progressTracker.endSubTask("Shuffle and Split");

        var trainingStatistics = new TrainingStatistics(List.copyOf(trainConfig.metrics()));

        selectBestModel(splits.innerSplits(), trainingStatistics);
        evaluateBestModel(splits.outerSplit(), trainingStatistics);

        var retrainedModel = retrainBestModel(splits.allTrainingExamples(), trainingStatistics.bestParameters());

        return ImmutableNodeRegressionTrainResult.of(retrainedModel, trainingStatistics);
    }

    private void selectBestModel(List<TrainingExamplesSplit> nodeSplits, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask("Select best model");

        var hyperParameterOptimizer = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.numberOfModelSelectionTrials(),
            trainConfig.randomSeed()
        );

        int trial = 0;
        while (hyperParameterOptimizer.hasNext()) {
            terminationFlag.assertRunning();

            progressTracker.beginSubTask("Trial");
            progressTracker.setSteps(nodeSplits.size());

            var modelParams = hyperParameterOptimizer.next();
            progressTracker.logMessage(formatWithLocale("Method: %s, Parameters: %s", modelParams.method(), modelParams.toMap()));

            var validationStatsBuilder = new ModelStatsBuilder(nodeSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(nodeSplits.size());

            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();

                var regressor = trainModel(trainSet, modelParams, ProgressTracker.NULL_TRACKER);

                registerMetricScores(validationSet, regressor, validationStatsBuilder::update);
                registerMetricScores(trainSet, regressor, trainStatsBuilder::update);

                progressTracker.logSteps(1);
            }

            var candidateStats = ModelCandidateStats.of(
                modelParams,
                trainStatsBuilder.build(),
                validationStatsBuilder.build()
            );
            trainingStatistics.addCandidateStats(candidateStats);

            progressTracker.logMessage(formatWithLocale(
                "Main validation metric (%s): %.4f",
                trainingStatistics.evaluationMetric(),
                trainingStatistics.getMainMetric(trial)
            ));
            progressTracker.logMessage(formatWithLocale("Validation metrics: %s",
                trainingStatistics.validationMetricsAvg(trial)
            ));
            progressTracker.logMessage(formatWithLocale("Training metrics: %s",
                trainingStatistics.trainMetricsAvg(trial)
            ));

            trial++;
            progressTracker.endSubTask("Trial");
        }
        progressTracker.endSubTask("Select best model");
    }

    private void registerMetricScores(
        HugeLongArray evaluationSet,
        Regressor regressor,
        BiConsumer<Metric, Double> scoreConsumer
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

        trainConfig.metrics().forEach(metric -> scoreConsumer.accept(metric, metric.compute(localTargets, localPredictions)));
    }

    private void evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("Train best model");
        var bestRegressor = trainModel(outerSplit.trainSet(), trainingStatistics.bestParameters(), progressTracker);
        progressTracker.endSubTask("Train best model");

        progressTracker.beginSubTask("Evaluate on test data");

        registerMetricScores(outerSplit.trainSet(), bestRegressor, trainingStatistics::addOuterTrainScore);
        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logMessage(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));

        registerMetricScores(outerSplit.testSet(), bestRegressor, trainingStatistics::addTestScore);
        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logMessage(formatWithLocale("Final model metrics on test set: %s", testMetrics));

        progressTracker.endSubTask("Evaluate on test data");
    }

    private Regressor retrainBestModel(HugeLongArray trainSet, TrainerConfig bestParameters) {
        progressTracker.beginSubTask("Retrain best model");
        var retrainedRegressor = trainModel(trainSet, bestParameters, progressTracker);
        progressTracker.endSubTask("Retrain best model");

        return retrainedRegressor;
    }

    private Regressor trainModel(
        HugeLongArray trainSet,
        TrainerConfig trainerConfig,
        ProgressTracker customProgressTracker
    ) {
        var trainer = RegressionTrainerFactory.create(
            trainerConfig,
            terminationFlag,
            customProgressTracker,
            trainConfig.concurrency(),
            trainConfig.randomSeed()
        );

        return trainer.train(features, targets, ReadOnlyHugeLongArray.of(trainSet));
    }

}
