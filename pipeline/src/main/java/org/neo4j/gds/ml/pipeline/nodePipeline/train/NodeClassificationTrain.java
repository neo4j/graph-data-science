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
import org.neo4j.gds.ml.metrics.BestMetricData;
import org.neo4j.gds.ml.metrics.ClassificationMetric;
import org.neo4j.gds.ml.metrics.ClassificationMetricSpecification;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.metrics.StatsMap;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.models.Trainer;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainerFactory;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.nodeClassification.ClassificationMetricComputer;
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.openjdk.jol.util.Multiset;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.pipeline.nodePipeline.train.LabelsAndClassCountsExtractor.extractLabelsAndClassCounts;
import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public final class NodeClassificationTrain {

    private final Graph graph;
    private final NodeClassificationPipelineTrainConfig config;
    private final NodeClassificationTrainingPipeline pipeline;
    private final Features features;
    private final HugeLongArray targets;
    private final LocalIdMap classIdMap;
    private final HugeLongArray nodeIds;
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
        NodeClassificationSplitConfig splitConfig = pipeline.splitConfig();
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

    public static Task progressTask(int validationFolds, int numberOfModelSelectionTrials) {
        return Tasks.task(
            "NCTrain",
            Tasks.leaf("ShuffleAndSplit"),
            Tasks.iterativeFixed(
                "SelectBestModel",
                () -> List.of(Tasks.iterativeFixed("Model Candidate", () -> List.of(
                        Tasks.task(
                            "Split",
                            Trainer.progressTask("Training"),
                            Tasks.leaf("Evaluate")
                        )
                    ), validationFolds)
                ),
                numberOfModelSelectionTrials
            ),
            Trainer.progressTask("TrainSelectedOnRemainder"),
            Tasks.leaf("EvaluateSelectedModel"),
            Trainer.progressTask("RetrainSelectedModel")
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
            .map(config -> {
                var training = TrainerFactory.memoryEstimation(
                    config,
                    trainSetSize,
                    fudgedClassCount,
                    MemoryRange.of(fudgedFeatureCount),
                    false
                );

                var evaluation = ClassificationMetricComputer.estimateEvaluation(
                    config,
                    trainSetSize,
                    testSetSize,
                    fudgedClassCount,
                    fudgedFeatureCount,
                    false
                );

                return MemoryEstimations.maxEstimation(List.of(training, evaluation));
            })
            .collect(Collectors.toList());

        return MemoryEstimations.builder("model selection")
            .max(foldEstimations)
            .build();
    }

    public static NodeClassificationTrain create(
        Graph graph,
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ProgressTracker progressTracker
    ) {
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());
        var labelsAndClassCounts = extractLabelsAndClassCounts(targetNodeProperty, graph.nodeCount());
        Multiset<Long> classCounts = labelsAndClassCounts.classCounts();
        HugeLongArray labels = labelsAndClassCounts.labels();
        var classIdMap = LocalIdMap.ofSorted(classCounts.keys());
        var metrics = config.metrics(classCounts.keys());
        var nodeIds = HugeLongArray.newArray(graph.nodeCount());
        nodeIds.setAll(i -> i);

        Features features;
        if (pipeline.trainingParameterSpace().get(TrainingMethod.RandomForest).isEmpty()) {
            features = FeaturesFactory.extractLazyFeatures(graph, pipeline.featureProperties());
        } else {
            // Random forest uses feature vectors many times each.
            features = FeaturesFactory.extractEagerFeatures(graph, pipeline.featureProperties());
        }

        var terminationFlag = TerminationFlag.RUNNING_TRUE;

        return new NodeClassificationTrain(
            graph,
            pipeline,
            config,
            features,
            labels,
            classIdMap,
            metrics,
            nodeIds,
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
        HugeLongArray nodeIds,
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
        this.nodeIds = nodeIds;
        this.classCounts = classCounts;
    }

    public NodeClassificationTrainResult compute() {
        progressTracker.beginSubTask();

        progressTracker.beginSubTask();
        ShuffleUtil.shuffleHugeLongArray(nodeIds, createRandomDataGenerator(config.randomSeed()));
        NodeClassificationSplitConfig splitConfig = pipeline.splitConfig();
        var outerSplit = new FractionSplitter().split(nodeIds, 1 - splitConfig.testFraction());
        var innerSplits = new StratifiedKFoldSplitter(
            splitConfig.validationFolds(),
            ReadOnlyHugeLongArray.of(outerSplit.trainSet()),
            ReadOnlyHugeLongArray.of(targets),
            config.randomSeed()
        ).splits();

        warnForSmallNodeSets(
            outerSplit.trainSet().size(),
            outerSplit.testSet().size(),
            splitConfig.validationFolds(),
            progressTracker
        );

        progressTracker.endSubTask();

        var trainingStatistics = new TrainingStatistics(List.copyOf(metrics));

        selectBestModel(innerSplits, trainingStatistics);
        Map<Metric, BestMetricData> metricResults = evaluateBestModel(outerSplit, trainingStatistics);

        Classifier retrainedModelData = retrainBestModel(trainingStatistics.bestParameters());

        progressTracker.endSubTask();

        return ImmutableNodeClassificationTrainResult.of(
            createModel(retrainedModelData, trainingStatistics, metricResults),
            trainingStatistics
        );
    }

    private void selectBestModel(List<TrainingExamplesSplit> nodeSplits, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask();

        var hyperParameterOptimizer = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.numberOfModelSelectionTrials(),
            config.randomSeed()
        );

        while (hyperParameterOptimizer.hasNext()) {
            var modelParams = hyperParameterOptimizer.next();
            progressTracker.beginSubTask();
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());

            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                progressTracker.beginSubTask();

                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();

                progressTracker.beginSubTask("Training");
                var classifier = trainModel(trainSet, modelParams);

                progressTracker.endSubTask("Training");

                progressTracker.beginSubTask(validationSet.size() + trainSet.size());
                registerMetricScores(validationSet, classifier, validationStatsBuilder::update);
                registerMetricScores(trainSet, classifier, trainStatsBuilder::update);
                progressTracker.endSubTask();

                progressTracker.endSubTask();
            }
            progressTracker.endSubTask();

            metrics.forEach(metric -> {
                trainingStatistics.addValidationStats(metric, validationStatsBuilder.build(metric));
                trainingStatistics.addTrainStats(metric, trainStatsBuilder.build(metric));
            });
        }
        progressTracker.endSubTask();
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
            progressTracker,
            terminationFlag
        );
        metrics.forEach(metric -> scoreConsumer.accept(metric, trainMetricComputer.score(metric)));
    }

    private Map<Metric, BestMetricData> evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        TrainingStatistics trainingStatistics
    ) {
        progressTracker.beginSubTask("TrainSelectedOnRemainder");
        var bestClassifier = trainModel(outerSplit.trainSet(), trainingStatistics.bestParameters());
        progressTracker.endSubTask("TrainSelectedOnRemainder");

        progressTracker.beginSubTask(outerSplit.testSet().size() + outerSplit.trainSet().size());
        registerMetricScores(outerSplit.testSet(), bestClassifier, trainingStatistics::addTestScore);
        registerMetricScores(outerSplit.trainSet(), bestClassifier, trainingStatistics::addOuterTrainScore);
        progressTracker.endSubTask();

        return trainingStatistics.metricsForWinningModel();
    }

    private Classifier retrainBestModel(TrainerConfig bestParameters) {
        progressTracker.beginSubTask("RetrainSelectedModel");
        var retrainedClassifier = trainModel(nodeIds, bestParameters);
        progressTracker.endSubTask("RetrainSelectedModel");

        return retrainedClassifier;
    }

    private Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> createModel(
        Classifier classifier,
        TrainingStatistics trainingStatistics,
        Map<Metric, BestMetricData> metricResults
    ) {

        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(classIdMap.originalIdsList())
            .bestParameters(trainingStatistics.bestParameters())
            .metrics(metricResults)
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
        TrainerConfig trainerConfig
    ) {
        Trainer trainer = TrainerFactory.create(
            trainerConfig,
            classIdMap,
            terminationFlag,
            progressTracker,
            config.concurrency(),
            config.randomSeed(),
            false
        );

        return trainer.train(features, targets, ReadOnlyHugeLongArray.of(trainSet));
    }

}
