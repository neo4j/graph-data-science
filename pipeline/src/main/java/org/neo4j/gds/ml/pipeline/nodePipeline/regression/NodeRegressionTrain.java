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
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.RegressionTrainerFactory;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;

import java.util.List;
import java.util.TreeSet;
import java.util.function.BiConsumer;

import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public final class NodeRegressionTrain {

    private final Features features;
    private final HugeDoubleArray targets;
    private final NodeRegressionTrainingPipeline pipeline;
    private final NodeRegressionPipelineTrainConfig trainConfig;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public static Task progressTask(int validationFolds, int numberOfModelSelectionTrials) {
        return Tasks.task(
            "Node Regression Train",
            Tasks.leaf("ShuffleAndSplit"),
            Tasks.iterativeFixed(
                "SelectBestModel",
                () -> List.of(Tasks.iterativeFixed("Model Candidate", () -> List.of(
                        Tasks.task(
                            "Split",
                            ClassifierTrainer.progressTask("Training"),
                            Tasks.leaf("Evaluate")
                        )
                    ), validationFolds)
                ),
                numberOfModelSelectionTrials
            ),
            ClassifierTrainer.progressTask("TrainSelectedOnRemainder"),
            Tasks.leaf("EvaluateSelectedModel"),
            ClassifierTrainer.progressTask("RetrainSelectedModel")
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
        if (pipeline.trainingParameterSpace().getOrDefault(TrainingMethod.RandomForest, List.of()).isEmpty()) {
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
        progressTracker.beginSubTask("Node Regression Train");

        progressTracker.beginSubTask("ShuffleAndSplit");
        var allTrainingExamples = HugeLongArray.newArray(features.size());
        allTrainingExamples.setAll(i -> i);

        ShuffleUtil.shuffleHugeLongArray(allTrainingExamples, createRandomDataGenerator(trainConfig.randomSeed()));
        NodePropertyPredictionSplitConfig splitConfig = pipeline.splitConfig();
        var outerSplit = new FractionSplitter().split(allTrainingExamples, 1 - splitConfig.testFraction());

        List<TrainingExamplesSplit> innerSplits = new StratifiedKFoldSplitter(
            splitConfig.validationFolds(),
            ReadOnlyHugeLongArray.of(outerSplit.trainSet()),
            id -> 0L,
            trainConfig.randomSeed(),
            new TreeSet<>(List.of(0L))
        ).splits();

        warnForSmallNodeSets(
            outerSplit.trainSet().size(),
            outerSplit.testSet().size(),
            splitConfig.validationFolds(),
            progressTracker
        );

        progressTracker.endSubTask("ShuffleAndSplit");

        var trainingStatistics = new TrainingStatistics(List.copyOf(trainConfig.metrics()));

        selectBestModel(innerSplits, trainingStatistics);
        evaluateBestModel(outerSplit, trainingStatistics);

        var retrainedModel = retrainBestModel(allTrainingExamples, trainingStatistics.bestParameters());

        progressTracker.endSubTask("Node Regression Train");

        return ImmutableNodeRegressionTrainResult.of(retrainedModel, trainingStatistics);
    }

    private void selectBestModel(List<TrainingExamplesSplit> nodeSplits, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask("SelectBestModel");

        var hyperParameterOptimizer = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.numberOfModelSelectionTrials(),
            trainConfig.randomSeed()
        );

        while (hyperParameterOptimizer.hasNext()) {
            var modelParams = hyperParameterOptimizer.next();
            progressTracker.beginSubTask("Model Candidate");
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());

            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                progressTracker.beginSubTask("Split");

                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();

                progressTracker.beginSubTask("Training");
                var regressor = trainModel(trainSet, modelParams);

                progressTracker.endSubTask("Training");

                progressTracker.beginSubTask("Evaluate",validationSet.size() + trainSet.size());
                registerMetricScores(validationSet, regressor, validationStatsBuilder::update);
                registerMetricScores(trainSet, regressor, trainStatsBuilder::update);
                progressTracker.endSubTask("Evaluate");

                progressTracker.endSubTask("Split");
            }
            progressTracker.endSubTask("Model Candidate");

            trainConfig.metrics().forEach(metric -> {
                trainingStatistics.addValidationStats(metric, validationStatsBuilder.build(metric));
                trainingStatistics.addTrainStats(metric, trainStatsBuilder.build(metric));
            });
        }
        progressTracker.endSubTask("SelectBestModel");
    }

    private void registerMetricScores(
        HugeLongArray evaluationSet,
        Regressor regressor,
        BiConsumer<Metric, Double> scoreConsumer
    ) {
        // TODO parallelize this part
        HugeDoubleArray predictions = HugeDoubleArray.newArray(evaluationSet.size());
        predictions.setAll(idx -> regressor.predict(features.get(evaluationSet.get(idx))));

        HugeDoubleArray localTargets = HugeDoubleArray.newArray(evaluationSet.size());
        localTargets.setAll(idx -> targets.get(evaluationSet.get(idx)));

        trainConfig.metrics().forEach(metric -> scoreConsumer.accept(metric, metric.compute(localTargets, predictions)));
    }

    private void evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("TrainSelectedOnRemainder");
        var bestClassifier = trainModel(outerSplit.trainSet(), trainingStatistics.bestParameters());
        progressTracker.endSubTask("TrainSelectedOnRemainder");

        progressTracker.beginSubTask("EvaluateSelectedModel", outerSplit.testSet().size() + outerSplit.trainSet().size());
        registerMetricScores(outerSplit.testSet(), bestClassifier, trainingStatistics::addTestScore);
        registerMetricScores(outerSplit.trainSet(), bestClassifier, trainingStatistics::addOuterTrainScore);
        progressTracker.endSubTask("EvaluateSelectedModel");
    }

    private Regressor retrainBestModel(HugeLongArray trainSet, TrainerConfig bestParameters) {
        progressTracker.beginSubTask("RetrainSelectedModel");
        var retrainedRegressor = trainModel(trainSet, bestParameters);
        progressTracker.endSubTask("RetrainSelectedModel");

        return retrainedRegressor;
    }

    private Regressor trainModel(
        HugeLongArray trainSet,
        TrainerConfig trainerConfig
    ) {
        var trainer = RegressionTrainerFactory.create(
            trainerConfig,
            terminationFlag,
            progressTracker,
            trainConfig.concurrency(),
            trainConfig.randomSeed()
        );

        return trainer.train(features, targets, ReadOnlyHugeLongArray.of(trainSet));
    }

}
