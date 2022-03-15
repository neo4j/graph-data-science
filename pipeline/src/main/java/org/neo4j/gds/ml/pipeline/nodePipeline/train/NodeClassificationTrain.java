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

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.gradientdescent.GradientDescentConfig;
import org.neo4j.gds.gradientdescent.Training;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.nodemodels.BestMetricData;
import org.neo4j.gds.ml.nodemodels.BestModelStats;
import org.neo4j.gds.ml.nodemodels.ClassificationMetricComputer;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.Metric;
import org.neo4j.gds.ml.nodemodels.MetricComputer;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.nodemodels.StatsMap;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationSplitConfig;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.gds.models.Classifier;
import org.neo4j.gds.models.Features;
import org.neo4j.gds.models.FeaturesFactory;
import org.neo4j.gds.models.Trainer;
import org.neo4j.gds.models.TrainerConfig;
import org.neo4j.gds.models.TrainerFactory;
import org.neo4j.gds.models.TrainingMethod;
import org.neo4j.gds.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainer;
import org.openjdk.jol.util.Multiset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public final class NodeClassificationTrain {

    private final Graph graph;
    private final NodeClassificationPipelineTrainConfig config;
    private final NodeClassificationPipeline pipeline;
    private final Features features;
    private final HugeLongArray targets;
    private final LocalIdMap classIdMap;
    private final HugeLongArray nodeIds;
    private final List<Metric> metrics;
    private final StatsMap trainStats;
    private final StatsMap validationStats;
    private final MetricComputer metricComputer;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public static MemoryEstimation estimate(NodeClassificationPipeline pipeline, NodeClassificationPipelineTrainConfig config) {
        var fudgedClassCount = 1000;
        var fudgedFeatureCount = 500;
        var holdoutFraction = pipeline.splitConfig().testFraction();
        var validationFolds = pipeline.splitConfig().validationFolds();

        // Can only derive this for LR atm.
        Optional<MemoryEstimation> maybeModelTrainingEstimation = Optional.empty();

        if (!pipeline.trainingParameterSpace().get(TrainingMethod.LogisticRegression).isEmpty()) {
            var maxBatchSize = pipeline.trainingParameterSpace().get(TrainingMethod.LogisticRegression)
                .stream()
                .mapToInt(trainConfig -> ((GradientDescentConfig) trainConfig).batchSize())
                .max()
                .orElseThrow();

            var modelSelection = modelTrainAndEvaluateMemoryUsage(
                maxBatchSize,
                fudgedClassCount,
                fudgedFeatureCount,
                (nodeCount) -> (long) (nodeCount * holdoutFraction * (validationFolds - 1) / validationFolds)
            );
            var bestModelEvaluation = delegateEstimation(
                modelTrainAndEvaluateMemoryUsage(
                    maxBatchSize,
                    fudgedClassCount,
                    fudgedFeatureCount,
                    (nodeCount) -> (long) (nodeCount * holdoutFraction)
                ),
                "best model evaluation"
            );

            maybeModelTrainingEstimation = Optional.of(maxEstimation(List.of(modelSelection, bestModelEvaluation)));
        }
        // Final step is to retrain the best model with the entire node set.
        // Training memory is independent of node set size so we can skip that last estimation.
        var builder = MemoryEstimations.builder()
            .perNode("global targets", HugeLongArray::memoryEstimation)
            .rangePerNode("global class counts", __ -> MemoryRange.of(2 * Long.BYTES, fudgedClassCount * Long.BYTES))
            .add("metrics", MetricSpecification.memoryEstimation(fudgedClassCount))
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .add("outer split", FractionSplitter.estimate(1 - holdoutFraction))
            .add("inner split", StratifiedKFoldSplitter.memoryEstimationForNodeSet(validationFolds, 1 - holdoutFraction))
            .add("stats map train", StatsMap.memoryEstimation(config.metrics().size(), pipeline.numberOfModelCandidates()))
            .add("stats map validation", StatsMap.memoryEstimation(config.metrics().size(), pipeline.numberOfModelCandidates()));

        if (maybeModelTrainingEstimation.isPresent()) {
            builder.add("max of model selection and best model evaluation", maybeModelTrainingEstimation.get());
        } else {
            // There's no memory estimation support for Random forest yet.
            builder.add(MemoryEstimations.of("max of model selection and best model evaluation (unknown)", MemoryRange.of(0)));
        }

        return builder.build();
    }

    public static String taskName() {
        return "NCTrain";
    }

    public static Task progressTask(int validationFolds, int paramsSize) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("ShuffleAndSplit"),
            Tasks.iterativeFixed(
                "SelectBestModel",
                () -> List.of(Tasks.iterativeFixed("Model Candidate", () -> List.of(
                        Tasks.task(
                            "Split",
                            Training.progressTask("Training"),
                            Tasks.leaf("Evaluate")
                        )
                    ), validationFolds)
                ),
                paramsSize
            ),
            Training.progressTask("TrainSelectedOnRemainder"),
            Tasks.leaf("EvaluateSelectedModel"),
            Training.progressTask("RetrainSelectedModel")
        );
    }

    @NotNull
    private static MemoryEstimation modelTrainAndEvaluateMemoryUsage(
        int maxBatchSize,
        int fudgedClassCount,
        int fudgedFeatureCount,
        LongUnaryOperator nodeSetSize
    ) {
        // There's no memory estimation support for Random forest yet.
        MemoryEstimation training = LogisticRegressionTrainer.memoryEstimation(
            fudgedClassCount,
            fudgedFeatureCount,
            maxBatchSize
        );

        MemoryEstimation metricsComputation = MemoryEstimations.builder("computing metrics")
            .perNode("local targets", (nodeCount) -> {
                var sizeOfLargePartOfAFold = nodeSetSize.applyAsLong(nodeCount);
                return HugeLongArray.memoryEstimation(sizeOfLargePartOfAFold);
            })
            .perNode("predicted classes", (nodeCount) -> {
                var sizeOfLargePartOfAFold = nodeSetSize.applyAsLong(nodeCount);
                return HugeLongArray.memoryEstimation(sizeOfLargePartOfAFold);
            })
            .fixed("probabilities", sizeOfDoubleArray(fudgedClassCount))
            .fixed("computation graph", LogisticRegressionClassifier.sizeOfPredictionsVariableInBytes(
                BatchQueue.DEFAULT_BATCH_SIZE,
                fudgedFeatureCount,
                fudgedClassCount
            ))
            .build();

        return MemoryEstimations.builder("model selection")
            .max(List.of(training, metricsComputation))
            .build();
    }

    public static NodeClassificationTrain create(
        Graph graph,
        NodeClassificationPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        ProgressTracker progressTracker
    ) {
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());
        var targetsAndClasses = computeGlobalTargetsAndClasses(targetNodeProperty, graph.nodeCount());
        var targets = targetsAndClasses.getOne();
        var classIdMap = makeClassIdMap(targets);
        var classCounts = targetsAndClasses.getTwo();
        var metrics = createMetrics(config, classCounts);
        var nodeIds = HugeLongArray.newArray(graph.nodeCount());
        nodeIds.setAll(i -> i);
        var trainStats = StatsMap.create(metrics);
        var validationStats = StatsMap.create(metrics);
        var features = FeaturesFactory.extractLazyFeatures(graph, pipeline.featureProperties());

        return new NodeClassificationTrain(
            graph,
            pipeline,
            config,
            features,
            targets,
            classIdMap,
            classCounts,
            metrics,
            nodeIds,
            trainStats,
            validationStats,
            progressTracker
        );
    }

    public static LocalIdMap makeClassIdMap(HugeLongArray targets) {
        var classSet = new TreeSet<Long>();
        var classIdMap = new LocalIdMap();
        for (long i = 0; i < targets.size(); i++) {
            classSet.add(targets.get(i));
        }
        classSet.forEach(classIdMap::toMapped);
        return classIdMap;
    }

    private static Pair<HugeLongArray, Multiset<Long>> computeGlobalTargetsAndClasses(
        NodeProperties targetNodeProperty,
        long nodeCount
    ) {
        var classCounts = new Multiset<Long>();
        var targets = HugeLongArray.newArray(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            targets.set(nodeId, targetNodeProperty.longValue(nodeId));
            classCounts.add(targetNodeProperty.longValue(nodeId));
        }
        return Tuples.pair(targets, classCounts);
    }

    private static List<Metric> createMetrics(NodeClassificationPipelineTrainConfig config, Multiset<Long> globalClassCounts) {
        return config.metrics()
            .stream()
            .flatMap(spec -> spec.createMetrics(globalClassCounts.keys()))
            .collect(Collectors.toList());
    }

    private NodeClassificationTrain(
        Graph graph,
        NodeClassificationPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        Features features,
        HugeLongArray targets,
        LocalIdMap classIdMap,
        Multiset<Long> classCounts,
        List<Metric> metrics,
        HugeLongArray nodeIds,
        StatsMap trainStats,
        StatsMap validationStats,
        ProgressTracker progressTracker
    ) {
        this.progressTracker = progressTracker;
        this.terminationFlag = TerminationFlag.RUNNING_TRUE;
        this.graph = graph;
        this.pipeline = pipeline;
        this.config = config;
        this.features = features;
        this.targets = targets;
        this.classIdMap = classIdMap;
        this.metrics = metrics;
        this.nodeIds = nodeIds;
        this.trainStats = trainStats;
        this.validationStats = validationStats;
        this.metricComputer = new ClassificationMetricComputer(
            metrics,
            classCounts,
            features,
            targets,
            config.concurrency(),
            progressTracker,
            terminationFlag
        );
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

        var modelSelectResult = selectBestModel(innerSplits);
        var bestParameters = modelSelectResult.bestParameters();
        Map<Metric, BestMetricData> metricResults = evaluateBestModel(outerSplit, modelSelectResult, bestParameters);

        Classifier retrainedModelData = retrainBestModel(bestParameters);

        progressTracker.endSubTask();

        return ImmutableNodeClassificationTrainResult.of(
            createModel(retrainedModelData, modelSelectResult, metricResults),
            modelSelectResult
        );
    }

    private ModelSelectResult selectBestModel(List<TrainingExamplesSplit> nodeSplits) {
        progressTracker.beginSubTask();
        pipeline.trainingParameterSpace().values().stream().flatMap(List::stream).forEach(modelParams -> {
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
                metricComputer.computeMetrics(validationSet, classifier).forEach(validationStatsBuilder::update);
                metricComputer.computeMetrics(trainSet, classifier).forEach(trainStatsBuilder::update);
                progressTracker.endSubTask();

                progressTracker.endSubTask();
            }
            progressTracker.endSubTask();

            metrics.forEach(metric -> {
                validationStats.add(metric, validationStatsBuilder.build(metric));
                trainStats.add(metric, trainStatsBuilder.build(metric));
            });
        });
        progressTracker.endSubTask();

        var mainMetric = metrics.get(0);
        var bestModelStats = validationStats.pickBestModelStats(mainMetric);

        return ModelSelectResult.of(bestModelStats.params(), trainStats, validationStats);
    }

    private Map<Metric, BestMetricData> evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        ModelSelectResult modelSelectResult,
        TrainerConfig bestParameters
    ) {
        progressTracker.beginSubTask("TrainSelectedOnRemainder");
        var bestClassifier = trainModel(outerSplit.trainSet(), bestParameters);
        progressTracker.endSubTask("TrainSelectedOnRemainder");

        progressTracker.beginSubTask(outerSplit.testSet().size() + outerSplit.trainSet().size());
        var testMetrics = metricComputer.computeMetrics(outerSplit.testSet(), bestClassifier);
        var outerTrainMetrics = metricComputer.computeMetrics(outerSplit.trainSet(), bestClassifier);
        progressTracker.endSubTask();

        return mergeMetricResults(modelSelectResult, outerTrainMetrics, testMetrics);
    }

    private Classifier retrainBestModel(TrainerConfig bestParameters) {
        progressTracker.beginSubTask("RetrainSelectedModel");
        var retrainedClassifier = trainModel(nodeIds, bestParameters);
        progressTracker.endSubTask("RetrainSelectedModel");

        return retrainedClassifier;
    }

    private Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> createModel(
        Classifier classifier,
        ModelSelectResult modelSelectResult,
        Map<Metric, BestMetricData> metricResults
    ) {

        var modelInfo = NodeClassificationPipelineModelInfo.builder()
            .classes(classIdMap.originalIdsList())
            .bestParameters(modelSelectResult.bestParameters())
            .metrics(metricResults)
            .trainingPipeline(pipeline.copy())
            .build();

        return Model.of(
            config.username(),
            config.modelName(),
            NodeClassificationPipeline.MODEL_TYPE,
            graph.schema(),
            classifier.data(),
            config,
            modelInfo
        );
    }

    private static Map<Metric, BestMetricData> mergeMetricResults(
        ModelSelectResult modelSelectResult,
        Map<Metric, Double> outerTrainMetrics,
        Map<Metric, Double> testMetrics
    ) {
        return modelSelectResult.validationStats().keySet().stream().collect(Collectors.toMap(
            Function.identity(),
            metric ->
                BestMetricData.of(
                    BestModelStats.findBestModelStats(modelSelectResult.trainStats().get(metric), modelSelectResult.bestParameters()),
                    BestModelStats.findBestModelStats(modelSelectResult.validationStats().get(metric), modelSelectResult.bestParameters()),
                    outerTrainMetrics.get(metric),
                    testMetrics.get(metric)
                )
        ));
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

    @ValueClass
    public interface ModelSelectResult {
        TrainerConfig bestParameters();
        Map<Metric, List<ModelStats>> trainStats();
        Map<Metric, List<ModelStats>> validationStats();

        static ModelSelectResult of(
            TrainerConfig bestConfig,
            StatsMap trainStats,
            StatsMap validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats.getMap(), validationStats.getMap());
        }

        @Value.Derived
        default Map<String, Object> toMap() {
            Function<Map<Metric, List<ModelStats>>, Map<String, Object>> statsConverter = stats ->
                stats.entrySet().stream().collect(Collectors.toMap(
                    entry -> entry.getKey().name(),
                    value -> value.getValue().stream().map(ModelStats::toMap)
                ));

            return Map.of(
                "bestParameters", bestParameters().toMap(),
                "trainStats", statsConverter.apply(trainStats()),
                "validationStats", statsConverter.apply(validationStats())
            );
        }

    }

    private static class ModelStatsBuilder {
        private final Map<Metric, Double> min;
        private final Map<Metric, Double> max;
        private final Map<Metric, Double> sum;
        private final TrainerConfig modelParams;
        private final int numberOfSplits;

        ModelStatsBuilder(TrainerConfig modelParams, int numberOfSplits) {
            this.modelParams = modelParams;
            this.numberOfSplits = numberOfSplits;
            this.min = new HashMap<>();
            this.max = new HashMap<>();
            this.sum = new HashMap<>();
        }

        void update(Metric metric, double value) {
            min.merge(metric, value, Math::min);
            max.merge(metric, value, Math::max);
            sum.merge(metric, value, Double::sum);
        }

        ModelStats build(Metric metric) {
            return ImmutableModelStats.of(
                modelParams,
                sum.get(metric) / numberOfSplits,
                min.get(metric),
                max.get(metric)
            );
        }
    }
}
