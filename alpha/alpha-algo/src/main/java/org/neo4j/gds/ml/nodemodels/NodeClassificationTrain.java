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
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionPredictor;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrain;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.NodeSplit;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class NodeClassificationTrain extends Algorithm<NodeClassificationTrain, Model<NodeLogisticRegressionData, NodeClassificationTrainConfig>> {

    public static final String MODEL_TYPE = "nodeLogisticRegression";

    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final HugeLongArray targets;
    private final Multiset<Long> classCounts;
    private final HugeLongArray nodeIds;
    private final AllocationTracker allocationTracker;
    private final List<Metric> metrics;
    private final StatsMap trainStats;
    private final StatsMap validationStats;

    static MemoryEstimation estimate(NodeClassificationTrainConfig config) {
        var maxBatchSize = config.params().stream()
            .map(params -> NodeLogisticRegressionTrainConfig.of(config.featureProperties(), config.targetProperty(), config.concurrency(), params).batchSize())
            .mapToInt(i -> i)
            .max()
            .getAsInt();
        var fudgedClassCount = 1000;
        var fudgedFeatureCount = 500;
        return MemoryEstimations.builder()
            .perNode("global targets", HugeLongArray::memoryEstimation)
            // there are between two and nodeCount classes, each is a long
            // TODO: class counts could be it's own tree to also capture the multiset itself
            .rangePerNode("global class counts", __ -> MemoryRange.of(2 * 8, fudgedClassCount * 8))
            .add("metrics", MetricSpecification.memoryEstimation(fudgedClassCount))
            .perNode("node IDs", HugeLongArray::memoryEstimation)
            .add("outer split", FractionSplitter.estimate(1 - config.holdoutFraction()))
            .add("inner split", StratifiedKFoldSplitter.memoryEstimation(config.validationFolds(), 1 - config.holdoutFraction()))
            .add("stats map train", StatsMap.memoryEstimation(config.metrics().size(), config.params().size()))
            .add("stats map validation", StatsMap.memoryEstimation(config.metrics().size(), config.params().size()))
            // TODO: Do we need to estimate the ModelStats and ModelStatsBuilder thingies?
            .add(
                "training",
                NodeLogisticRegressionTrain.memoryEstimation(
                    fudgedClassCount,
                    fudgedFeatureCount,
                    maxBatchSize,
                    TrainingConfig.DEFAULT_SHARED_UPDATER
                )
            )
            .build();
    }

    public static NodeClassificationTrain create(
        Graph graph,
        NodeClassificationTrainConfig config,
        AllocationTracker allocationTracker,
        ProgressLogger progressLogger
    ) {
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());
        var targetsAndClasses = computeGlobalTargetsAndClasses(targetNodeProperty, graph.nodeCount(), allocationTracker);
        var targets = targetsAndClasses.getOne();
        var classCounts = targetsAndClasses.getTwo();
        var metrics = createMetrics(config, classCounts);
        var nodeIds = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        nodeIds.setAll(i -> i);
        var trainStats = StatsMap.create(metrics);
        var validationStats = StatsMap.create(metrics);
        return new NodeClassificationTrain(graph, config, targets, classCounts, metrics, nodeIds, trainStats, validationStats, allocationTracker, progressLogger);
    }

    private static Pair<HugeLongArray, Multiset<Long>> computeGlobalTargetsAndClasses(NodeProperties targetNodeProperty, long nodeCount, AllocationTracker allocationTracker) {
        var classCounts = new Multiset<Long>();
        var targets = HugeLongArray.newArray(nodeCount, allocationTracker);
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
        HugeLongArray targets,
        Multiset<Long> classCounts,
        List<Metric> metrics,
        HugeLongArray nodeIds,
        StatsMap trainStats,
        StatsMap validationStats,
        AllocationTracker allocationTracker,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.config = config;
        this.targets = targets;
        this.classCounts = classCounts;
        this.metrics = metrics;
        this.nodeIds = nodeIds;
        this.trainStats = trainStats;
        this.validationStats = validationStats;
        this.allocationTracker = allocationTracker;
        this.progressLogger = progressLogger;
    }

    @Override
    public NodeClassificationTrain me() {
        return this;
    }

    @Override
    public void release() {}

    @Override
    public Model<NodeLogisticRegressionData, NodeClassificationTrainConfig> compute() {
        progressLogger.logStart(":: Shuffle and Split");
        ShuffleUtil.shuffleHugeLongArray(nodeIds, createRandomDataGenerator(config.randomSeed()));
        var outerSplit = new FractionSplitter(allocationTracker).split(nodeIds, 1 - config.holdoutFraction());
        var innerSplits = new StratifiedKFoldSplitter(config.validationFolds(), outerSplit.trainSet(), targets, config.randomSeed()).splits();
        progressLogger.logFinish(":: Shuffle and Split");

        var modelSelectResult = selectBestModel(innerSplits);
        var bestParameters = modelSelectResult.bestParameters();
        var metricResults = evaluateBestModel(outerSplit, modelSelectResult, bestParameters);

        var retrainedModelData = retrainBestModel(bestParameters);

        return createModel(bestParameters, metricResults, retrainedModelData);
    }

    private ModelSelectResult selectBestModel(List<NodeSplit> splits) {
        var paramConfigCounter = 1;
        for (var modelParams : config.paramsConfig()) {
            var candidateMessage = formatWithLocale(":: Model Candidate %s of %s", paramConfigCounter++, config.paramsConfig().size());
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            for (int j = 0; j < splits.size(); j++) {
                var split = splits.get(j);
                var candidateAndSplitMessage = formatWithLocale(candidateMessage + " :: Split %s of %s", j + 1, splits.size());

                var trainSet = split.trainSet();
                var validationSet = split.testSet();

                progressLogger.logStart(candidateAndSplitMessage + " :: Train");
                // The best upper bound we have for bounding progress is maxEpochs, so tell the user what that value is
                int maxEpochs = modelParams.maxEpochs();
                progressLogger.logMessage(formatWithLocale(
                    candidateAndSplitMessage + " :: Train :: Max iterations: %s",
                    maxEpochs
                ));
                progressLogger.reset(maxEpochs);
                var modelData = trainModel(trainSet, modelParams);
                progressLogger.logFinish(candidateAndSplitMessage + " :: Train");

                progressLogger.logStart(candidateAndSplitMessage + " :: Evaluate");
                progressLogger.reset(validationSet.size() + trainSet.size());
                computeMetrics(classCounts, validationSet, modelData, metrics).forEach(validationStatsBuilder::update);
                computeMetrics(classCounts, trainSet, modelData, metrics).forEach(trainStatsBuilder::update);
                progressLogger.logFinish(candidateAndSplitMessage + " :: Evaluate");
            }
            metrics.forEach(metric -> {
                validationStats.add(metric, validationStatsBuilder.build(metric));
                trainStats.add(metric, trainStatsBuilder.build(metric));
            });
        }

        progressLogger.logStart(":: Select Model");
        var mainMetric = metrics.get(0);
        var bestModelStats = validationStats.pickBestModelStats(mainMetric);
        progressLogger.logFinish(":: Select Model");

        return ModelSelectResult.of(bestModelStats.params(), trainStats, validationStats);
    }

    private Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> evaluateBestModel(
        NodeSplit outerSplit,
        ModelSelectResult modelSelectResult,
        NodeLogisticRegressionTrainConfig bestParameters
    ) {
        int maxEpochs = bestParameters.maxEpochs();
        progressLogger.logStart(":: Train Selected on Remainder");
        progressLogger.reset(maxEpochs);
        NodeLogisticRegressionData bestModelData = trainModel(outerSplit.trainSet(), bestParameters);
        progressLogger.logFinish(":: Train Selected on Remainder");

        progressLogger.logStart(":: Evaluate Selected Model");
        progressLogger.reset(outerSplit.testSet().size() + outerSplit.trainSet().size());
        var testMetrics = computeMetrics(classCounts, outerSplit.testSet(), bestModelData, metrics);
        var outerTrainMetrics = computeMetrics(classCounts, outerSplit.trainSet(), bestModelData, metrics);
        progressLogger.logFinish(":: Evaluate Selected Model");

        return mergeMetricResults(modelSelectResult, outerTrainMetrics, testMetrics);
    }

    private NodeLogisticRegressionData retrainBestModel(NodeLogisticRegressionTrainConfig bestParameters) {
        int maxEpochs = bestParameters.maxEpochs();
        progressLogger.logStart(":: Retrain Selected Model");
        progressLogger.reset(maxEpochs);
        var retrainedModelData = trainModel(nodeIds, bestParameters);
        progressLogger.logFinish(":: Retrain Selected Model");
        return retrainedModelData;
    }

    private Model<NodeLogisticRegressionData, NodeClassificationTrainConfig> createModel(
        NodeLogisticRegressionTrainConfig bestParameters,
        Map<Metric, MetricData<NodeLogisticRegressionTrainConfig>> metricResults,
        NodeLogisticRegressionData retrainedModelData
    ) {
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
            metric -> metric,
            metric ->
                MetricData.of(
                    modelSelectResult.trainStats().get(metric),
                    modelSelectResult.validationStats().get(metric),
                    outerTrainMetrics.get(metric),
                    testMetrics.get(metric)
                )
        ));
    }

    private Map<Metric, List<ModelStats<NodeLogisticRegressionTrainConfig>>> initStatsMap(Iterable<Metric> metrics) {
        var statsMap = new HashMap<Metric, List<ModelStats<NodeLogisticRegressionTrainConfig>>>();
        metrics.forEach(metric -> statsMap.put(metric, new ArrayList<>()));
        return statsMap;
    }

    private Map<Metric, Double> computeMetrics(
        Multiset<Long> globalClassCounts,
        HugeLongArray evaluationSet,
        NodeLogisticRegressionData modelData,
        Collection<Metric> metrics
    ) {
        var predictor = new NodeLogisticRegressionPredictor(modelData, config.featureProperties());
        var predictedClasses = HugeLongArray.newArray(evaluationSet.size(), allocationTracker);

        // consume from queue which contains local nodeIds, i.e. indices into evaluationSet
        // the consumer internally remaps to original nodeIds before prediction
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            evaluationSet::get,
            predictor,
            null,
            predictedClasses,
            config.featureProperties(),
            progressLogger
        );

        var queue = new BatchQueue(evaluationSet.size());
        queue.parallelConsume(consumer, config.concurrency());

        var localTargets = makeLocalTargets(evaluationSet);
        return metrics.stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> metric.compute(localTargets, predictedClasses, globalClassCounts)
        ));
    }

    private NodeLogisticRegressionData trainModel(
        HugeLongArray trainSet,
        NodeLogisticRegressionTrainConfig nlrConfig
    ) {
        var train = new NodeLogisticRegressionTrain(graph, trainSet, nlrConfig, progressLogger);
        return train.compute();
    }

    private HugeLongArray makeLocalTargets(HugeLongArray nodeIds) {
        var targets = HugeLongArray.newArray(nodeIds.size(), allocationTracker);
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());

        targets.setAll(i -> targetNodeProperty.longValue(nodeIds.get(i)));
        return targets;
    }

    @ValueClass
    interface ModelSelectResult {
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
