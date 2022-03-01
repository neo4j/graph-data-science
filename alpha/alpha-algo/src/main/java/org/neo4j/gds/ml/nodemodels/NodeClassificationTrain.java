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
package org.neo4j.gds.ml.nodemodels;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.Features;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainConfigImpl;
import org.neo4j.gds.ml.logisticregression.LogisticRegressionTrainer;
import org.neo4j.gds.ml.nodemodels.logisticregression.ImmutableNodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrain;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.openjdk.jol.util.Multiset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.delegateEstimation;
import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.gds.ml.util.TrainingSetWarnings.warnForSmallNodeSets;

public final class NodeClassificationTrain extends Algorithm<Model<NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo>> {

    public static final String MODEL_TYPE = "nodeLogisticRegression";

    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final Features features;
    private final HugeLongArray targets;
    private final LocalIdMap classIdMap;
    private final HugeLongArray nodeIds;
    private final List<Metric> metrics;
    private final StatsMap trainStats;
    private final StatsMap validationStats;
    private final MetricComputer metricComputer;

    public static MemoryEstimation estimate(NodeClassificationTrainConfig config) {
        var maxBatchSize = config.paramsConfig()
            .stream()
            .mapToInt(TrainingConfig::batchSize)
            .max()
            .getAsInt();
        var fudgedClassCount = 1000;
        var fudgedFeatureCount = 500;
        var holdoutFraction = config.holdoutFraction();
        var validationFolds = config.validationFolds();

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
        var maxOfModelSelectionAndBestModelEvaluation = maxEstimation(List.of(modelSelection, bestModelEvaluation));
        // Final step is to retrain the best model with the entire node set.
        // Training memory is independent of node set size so we can skip that last estimation.
        return MemoryEstimations.builder()
            .perNode("global targets", HugeLongArray::memoryEstimation)
            .rangePerNode("global class counts", __ -> MemoryRange.of(2 * Long.BYTES, fudgedClassCount * Long.BYTES))
            .add("metrics", MetricSpecification.memoryEstimation(fudgedClassCount))
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .add("outer split", FractionSplitter.estimate(1 - holdoutFraction))
            .add("inner split", StratifiedKFoldSplitter.memoryEstimationForNodeSet(validationFolds, 1 - holdoutFraction))
            .add("stats map train", StatsMap.memoryEstimation(config.metrics().size(), config.params().size()))
            .add("stats map validation", StatsMap.memoryEstimation(config.metrics().size(), config.params().size()))
            .add("max of model selection and best model evaluation", maxOfModelSelectionAndBestModelEvaluation)
            .build();
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
        MemoryEstimation training = NodeLogisticRegressionTrain.memoryEstimation(
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
            .fixed("computation graph", NodeLogisticRegressionPredictor.sizeOfPredictionsVariableInBytes(
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
        NodeClassificationTrainConfig config,
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
        var features = Features.extractLazyFeatures(graph, config.featureProperties());

        return new NodeClassificationTrain(
            graph,
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

    private static List<Metric> createMetrics(NodeClassificationTrainConfig config, Multiset<Long> globalClassCounts) {
        return config.metrics()
            .stream()
            .flatMap(spec -> spec.createMetrics(globalClassCounts.keys()))
            .collect(Collectors.toList());
    }

    private NodeClassificationTrain(
        Graph graph,
        NodeClassificationTrainConfig config,
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
        super(progressTracker);
        this.graph = graph;
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
            graph,
            config,
            progressTracker,
            terminationFlag
        );
    }

    @Override
    public void release() {}

    @Override
    public Model<NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo> compute() {
        progressTracker.beginSubTask();

        progressTracker.beginSubTask();
        ShuffleUtil.shuffleHugeLongArray(nodeIds, createRandomDataGenerator(config.randomSeed()));
        var outerSplit = new FractionSplitter().split(nodeIds, 1 - config.holdoutFraction());
        var innerSplits = new StratifiedKFoldSplitter(
            config.validationFolds(),
            ReadOnlyHugeLongArray.of(outerSplit.trainSet()),
            ReadOnlyHugeLongArray.of(targets),
            config.randomSeed()
        ).splits();

        warnForSmallNodeSets(
            outerSplit.trainSet().size(),
            outerSplit.testSet().size(),
            config.validationFolds(),
            progressTracker
        );

        progressTracker.endSubTask();

        var modelSelectResult = selectBestModel(innerSplits);
        var bestParameters = modelSelectResult.bestParameters();
        var metricResults = evaluateBestModel(outerSplit, modelSelectResult, bestParameters);

        var retrainedModelData = retrainBestModel(bestParameters);
        progressTracker.endSubTask();

        return createModel(bestParameters, metricResults, retrainedModelData);
    }

    private ModelSelectResult selectBestModel(List<TrainingExamplesSplit> nodeSplits) {
        progressTracker.beginSubTask();
        for (NodeLogisticRegressionTrainConfig modelParams : config.paramsConfig()) {
            progressTracker.beginSubTask();
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, nodeSplits.size());

            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                progressTracker.beginSubTask();

                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();

                progressTracker.beginSubTask("Training");
                var classifier = trainModel(trainSet, modelParams);

                var predictor = convertClassifier(classifier);
                progressTracker.endSubTask("Training");

                progressTracker.beginSubTask(validationSet.size() + trainSet.size());
                metricComputer.computeMetrics(validationSet, predictor).forEach(validationStatsBuilder::update);
                metricComputer.computeMetrics(trainSet, predictor).forEach(trainStatsBuilder::update);
                progressTracker.endSubTask();

                progressTracker.endSubTask();
            }
            progressTracker.endSubTask();

            metrics.forEach(metric -> {
                validationStats.add(metric, validationStatsBuilder.build(metric));
                trainStats.add(metric, trainStatsBuilder.build(metric));
            });
        }
        progressTracker.endSubTask();

        var mainMetric = metrics.get(0);
        var bestModelStats = validationStats.pickBestModelStats(mainMetric);

        return ModelSelectResult.of(bestModelStats.params(), trainStats, validationStats);
    }

    @NotNull
    private NodeLogisticRegressionPredictor convertClassifier(LogisticRegressionClassifier classifier) {
        var weights = classifier.data().weights().data().data();
        var bias = classifier.data().bias().orElseThrow().data().data();

        var weightsWithBias = new double[weights.length + bias.length];
        for (int i = 0; i < weightsWithBias.length; i++) {
            int numberOfColumns = weights.length / bias.length + 1;
            int currentColumn = i % numberOfColumns;
            if (currentColumn == numberOfColumns - 1) {
                weightsWithBias[i] = bias[i / numberOfColumns];
            } else {
                weightsWithBias[i] = weights[i - i / numberOfColumns];
            }
        }

        var builder = ImmutableNodeLogisticRegressionData.builder()
            .classIdMap(classifier.classIdMap())
            .weights(new Weights<>(new Matrix(weightsWithBias, classifier.data().weights().data().rows(), classifier.data().weights().data().cols() + 1)));
        return new NodeLogisticRegressionPredictor(builder.build(), config.featureProperties());
    }

    private LogisticRegressionTrainConfig convertConfig(NodeLogisticRegressionTrainConfig nlrConfig) {
        return LogisticRegressionTrainConfigImpl.builder()
            .batchSize(nlrConfig.batchSize())
            .maxEpochs(nlrConfig.maxEpochs())
            .minEpochs(nlrConfig.minEpochs())
            .patience(nlrConfig.patience())
            .penalty(nlrConfig.penalty())
            .tolerance(nlrConfig.tolerance())
            .useBiasFeature(true).build();
    }

    private Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> evaluateBestModel(
        TrainingExamplesSplit outerSplit,
        ModelSelectResult modelSelectResult,
        NodeLogisticRegressionTrainConfig bestParameters
    ) {
        progressTracker.beginSubTask("TrainSelectedOnRemainder");
        var bestClassifier = trainModel(outerSplit.trainSet(), bestParameters);
        progressTracker.endSubTask("TrainSelectedOnRemainder");

        progressTracker.beginSubTask(outerSplit.testSet().size() + outerSplit.trainSet().size());
        var builder = ImmutableNodeLogisticRegressionData.builder()
            .classIdMap(bestClassifier.classIdMap())
            .weights(bestClassifier.data().weights());
        bestClassifier.data().bias().ifPresent(builder::bias);
        var testMetrics = metricComputer.computeMetrics(outerSplit.testSet(), convertClassifier(bestClassifier));
        var outerTrainMetrics = metricComputer.computeMetrics(outerSplit.trainSet(), convertClassifier(bestClassifier));
        progressTracker.endSubTask();

        return mergeMetricResults(modelSelectResult, outerTrainMetrics, testMetrics);
    }

    private LogisticRegressionClassifier retrainBestModel(NodeLogisticRegressionTrainConfig bestParameters) {
        progressTracker.beginSubTask("RetrainSelectedModel");
        var retrainedClassifier = trainModel(nodeIds, bestParameters);
        progressTracker.endSubTask("RetrainSelectedModel");
        return retrainedClassifier;
    }

    private Model<NodeLogisticRegressionData, NodeClassificationTrainConfig, NodeClassificationModelInfo> createModel(
        NodeLogisticRegressionTrainConfig bestParameters,
        Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> metricResults,
        LogisticRegressionClassifier retrainedClassifier
    ) {
        var builder = ImmutableNodeLogisticRegressionData.builder()
            .weights(retrainedClassifier.data().weights())
            .classIdMap(retrainedClassifier.data().classIdMap());
        retrainedClassifier.data().bias().ifPresent(bias -> builder.bias(bias));
        NodeLogisticRegressionData retrainedModelData = builder.build();
        var modelInfo = NodeClassificationModelInfo.of(
            retrainedModelData.classIdMap().originalIdsList(),
            bestParameters,
            metricResults
        );

        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            graph.schema(),
            retrainedModelData,
            config,
            modelInfo
        );
    }

    private Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> mergeMetricResults(
        ModelSelectResult modelSelectResult,
        Map<Metric, Double> outerTrainMetrics,
        Map<Metric, Double> testMetrics
    ) {
        return modelSelectResult.validationStats().keySet().stream().collect(Collectors.toMap(
            Function.identity(),
            metric ->
                MetricData.of(
                    modelSelectResult.trainStats().get(metric),
                    modelSelectResult.validationStats().get(metric),
                    outerTrainMetrics.get(metric),
                    testMetrics.get(metric)
                )
        ));
    }

    private LogisticRegressionClassifier trainModel(
        HugeLongArray trainSet,
        NodeLogisticRegressionTrainConfig nlrConfig
    ) {
        var trainer = new LogisticRegressionTrainer(
            ReadOnlyHugeLongArray.of(trainSet),
            config.concurrency(),
            convertConfig(nlrConfig),
            classIdMap,
            false,
            terminationFlag,
            progressTracker
        );
        return trainer.train(features, targets);
    }

    @ValueClass
    public interface ModelSelectResult {
        NodeLogisticRegressionTrainConfig bestParameters();
        Map<Metric, List<ModelStats<NodeLogisticRegressionTrainConfig>>> trainStats();
        Map<Metric, List<ModelStats<NodeLogisticRegressionTrainConfig>>> validationStats();

        static ModelSelectResult of(
            NodeLogisticRegressionTrainConfig bestConfig,
            StatsMap trainStats,
            StatsMap validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats.getMap(), validationStats.getMap());
        }

        @Value.Derived
        default Map<String, Object> toMap() {
            Function<Map<Metric, List<ModelStats<NodeLogisticRegressionTrainConfig>>>, Map<String, Object>> statsConverter = stats ->
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
        private final NodeLogisticRegressionTrainConfig modelParams;
        private final int numberOfSplits;

        ModelStatsBuilder(NodeLogisticRegressionTrainConfig modelParams, int numberOfSplits) {
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

        ModelStats<NodeLogisticRegressionTrainConfig> build(Metric metric) {
            return ImmutableModelStats.of(
                modelParams,
                sum.get(metric) / numberOfSplits,
                min.get(metric),
                max.get(metric)
            );
        }
    }
}
