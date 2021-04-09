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

import org.apache.commons.math3.random.RandomDataGenerator;
import org.neo4j.gds.ml.TrainingConfig;
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRPredictor;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrainConfig;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class NodeClassificationTrain
    extends Algorithm<NodeClassificationTrain, Model<MultiClassNLRData, NodeClassificationTrainConfig>> {

    public static final String MODEL_TYPE = "multiClassNodeLogisticRegression";

    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final AllocationTracker allocationTracker;

    public NodeClassificationTrain(
        Graph graph,
        NodeClassificationTrainConfig config,
        AllocationTracker allocationTracker,
        ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.config = config;
        this.allocationTracker = allocationTracker;
        this.progressLogger = progressLogger;
    }


    @Override
    public Model<MultiClassNLRData, NodeClassificationTrainConfig> compute() {
        var globalTargets = makeGlobalTargets();
        var globalClassCounts = countClassesGlobally();
        var metrics = createMetrics(globalClassCounts);

        // 1. Init and shuffle node ids
        progressLogger.logStart(":: Shuffle and Split");
        var nodeIds = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        nodeIds.setAll(i -> i);
        ShuffleUtil.shuffleHugeLongArray(nodeIds, getRandomDataGenerator());

        // 2a. Outer split nodes into holdout + remaining
        var outerSplitter = new FractionSplitter();
        var outerSplit = outerSplitter.split(nodeIds, 1 - config.holdoutFraction());

        // 2b. Inner split: enumerate a number of train/validation splits of remaining
        var splitter = new StratifiedKFoldSplitter(config.validationFolds(), outerSplit.trainSet(), globalTargets, config.randomSeed());
        var splits = splitter.splits();
        progressLogger.logFinish(":: Shuffle and Split");

        var trainStats = initStatsMap(metrics);
        var validationStats = initStatsMap(metrics);

        for (int i = 0; i < config.params().size(); i++) {
            var candidateMessage = formatWithLocale(":: Model Candidate %s of %s", i + 1, config.params().size());
            var modelParams = config.params().get(i);
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            for (int j = 0; j < splits.size(); j++) {
                var split = splits.get(j);
                var candidateAndSplitMessage = formatWithLocale(candidateMessage + " :: Split %s of %s", j + 1, splits.size());

                var trainSet = split.trainSet();
                var validationSet = split.testSet();

                // 3. train each model candidate on the train sets
                progressLogger.logStart(candidateAndSplitMessage + " :: Train");
                // The best upper bound we have for bounding progress is maxIterations, so tell the user what that value is
                // TODO: we are circumventing type checking built into config creation
                //       could we trigger this type checking earlier? there is a lot of distance between this interaction and the
                //       actual trainin code, can we close the gap? could we create the TrainConfig here?
                //       it's nice to have all the progress logging configuration here, but if we can't close the gap it may be
                //       better to push it (back) down into the train logic
                int maxIterations = ((Number) modelParams.getOrDefault("maxIterations", TrainingConfig.MAX_ITERATIONS)).intValue();
                progressLogger.logMessage(formatWithLocale(
                    candidateAndSplitMessage + " :: Train :: Max iterations: %s",
                    maxIterations
                ));
                progressLogger.reset(maxIterations);
                var modelData = trainModel(trainSet, modelParams);
                progressLogger.logFinish(candidateAndSplitMessage + " :: Train");

                // 4. evaluate each model candidate on the train and validation sets
                progressLogger.logStart(candidateAndSplitMessage + " :: Evaluate");
                progressLogger.reset(validationSet.size() + trainSet.size());
                computeMetrics(globalClassCounts, validationSet, modelData, metrics).forEach(validationStatsBuilder::update);
                computeMetrics(globalClassCounts, trainSet, modelData, metrics).forEach(trainStatsBuilder::update);
                progressLogger.logFinish(candidateAndSplitMessage + " :: Evaluate");
            }
            // insert the candidates metrics into trainStats and validationStats
            metrics.forEach(metric -> {
                validationStats.get(metric).add(validationStatsBuilder.modelStats(metric));
                trainStats.get(metric).add(trainStatsBuilder.modelStats(metric));
            });
        }

        progressLogger.logStart(":: Select Model");
        // 5. pick the best-scoring model candidate, according to the main metric
        var mainMetric = metrics.get(0);
        var modelStats = validationStats.get(mainMetric);
        var winner = Collections.max(modelStats, COMPARE_AVERAGE);
        progressLogger.logFinish(":: Select Model");

        var bestConfig = winner.params();
        int maxIterations = ((Number) bestConfig.getOrDefault("maxIterations", TrainingConfig.MAX_ITERATIONS)).intValue();
        var modelSelectResult = ModelSelectResult.of(bestConfig, trainStats, validationStats);

        var bestParameters = modelSelectResult.bestParameters();

        // 6. train best model on remaining
        progressLogger.logStart(":: Train Selected on Remainder");
        progressLogger.reset(maxIterations);
        MultiClassNLRData winnerModelData = trainModel(outerSplit.trainSet(), bestParameters);
        progressLogger.logFinish(":: Train Selected on Remainder");

        // 7. evaluate it on the holdout set and outer training set
        progressLogger.logStart(":: Evaluate Selected Model");
        progressLogger.reset(outerSplit.testSet().size() + outerSplit.trainSet().size());
        var testMetrics = computeMetrics(globalClassCounts, outerSplit.testSet(), winnerModelData, metrics);
        var outerTrainMetrics = computeMetrics(globalClassCounts, outerSplit.trainSet(), winnerModelData, metrics);
        progressLogger.logFinish(":: Evaluate Selected Model");

        // we are done with all metrics!
        var metricResults = mergeMetricResults(modelSelectResult, outerTrainMetrics, testMetrics);

        // 8. retrain that model on the full graph
        progressLogger.logStart(":: Retrain Selected Model");
        progressLogger.reset(maxIterations);
        MultiClassNLRData retrainedModelData = trainModel(nodeIds, bestParameters);
        progressLogger.logFinish(":: Retrain Selected Model");

        var modelInfo = NodeClassificationModelInfo.of(
            retrainedModelData.classIdMap().originalIdsList(),
            modelSelectResult.bestParameters(),
            metricResults
        );

        // 9. persist the winning model in the model catalog
        //    the model catalog will also contain training metadata
        //    such as min/max/avg scores of the input model candidate configs
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

    private List<Metric> createMetrics(Multiset<Long> globalClassCounts) {
        return config.metrics()
            .stream()
            .flatMap(spec -> spec.createMetrics(globalClassCounts.keys()))
            .collect(Collectors.toList());
    }

    private RandomDataGenerator getRandomDataGenerator() {
        var random = new RandomDataGenerator();
        config.randomSeed().ifPresent(random::reSeed);
        return random;
    }

    private Map<Metric, MetricData> mergeMetricResults(
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

    private Map<Metric, List<ModelStats>> initStatsMap(List<Metric> metrics) {
        var statsMap = new HashMap<Metric, List<ModelStats>>();
        metrics.forEach(metric -> statsMap.put(metric, new ArrayList<>()));
        return statsMap;
    }

    private Map<Metric, Double> computeMetrics(
        Multiset<Long> globalClassCounts,
        HugeLongArray evaluationSet,
        MultiClassNLRData modelData,
        List<Metric> metrics
    ) {
        var localTargets = makeLocalTargets(evaluationSet);

        var predictedClasses = HugeLongArray.newArray(evaluationSet.size(), allocationTracker);

        // consume from queue which contains local nodeIds, i.e. indices into evaluationSet
        // the consumer internally remaps to original nodeIds before prediction
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            evaluationSet::get,
            predictor(modelData),
            null,
            predictedClasses,
            config.featureProperties(),
            progressLogger
        );

        var queue = new BatchQueue(evaluationSet.size());
        queue.parallelConsume(consumer, config.concurrency());

        return metrics.stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> metric.compute(localTargets, predictedClasses, globalClassCounts)
        ));
    }

    private MultiClassNLRPredictor predictor(MultiClassNLRData modelData) {
        return new MultiClassNLRPredictor(modelData, config.featureProperties());
    }

    private MultiClassNLRData trainModel(
        HugeLongArray trainSet,
        Map<String, Object> modelParams
    ) {
        var nlrConfig = MultiClassNLRTrainConfig.of(
            config.featureProperties(),
            config.targetProperty(),
            config.concurrency(),
            modelParams
        );
        var train = new MultiClassNLRTrain(graph, trainSet, nlrConfig, progressLogger);
        return train.compute();
    }

    private Multiset<Long> countClassesGlobally() {
        var globalClassCounts = new Multiset<Long>();
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());
        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            globalClassCounts.add(targetNodeProperty.longValue(nodeId));
        }
        return globalClassCounts;
    }

    private HugeLongArray makeGlobalTargets() {
        var targets = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());

        targets.setAll(targetNodeProperty::longValue);
        return targets;
    }

    private HugeLongArray makeLocalTargets(HugeLongArray nodeIds) {
        var targets = HugeLongArray.newArray(nodeIds.size(), allocationTracker);
        var targetNodeProperty = graph.nodeProperties(config.targetProperty());

        targets.setAll(i -> targetNodeProperty.longValue(nodeIds.get(i)));
        return targets;
    }

    @Override
    public NodeClassificationTrain me() {
        return this;
    }

    @Override
    public void release() {

    }

    @ValueClass
    interface ModelSelectResult {
        Map<String, Object> bestParameters();

        // key is metric
        Map<Metric, List<ModelStats>> trainStats();
        // key is metric
        Map<Metric, List<ModelStats>> validationStats();

        static ModelSelectResult of(
            Map<String, Object> bestConfig,
            Map<Metric, List<ModelStats>> trainStats,
            Map<Metric, List<ModelStats>> validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
        }

    }

    private class ModelStatsBuilder {
        private final Map<Metric, Double> min;
        private final Map<Metric, Double> max;
        private final Map<Metric, Double> sum;
        private final Map<String, Object> modelParams;
        private final int numberOfSplits;

        ModelStatsBuilder(Map<String, Object> modelParams, int numberOfSplits) {
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

        ModelStats modelStats(Metric metric) {
            return ImmutableModelStats.of(
                modelParams,
                sum.get(metric) / numberOfSplits,
                min.get(metric),
                max.get(metric)
            );
        }
    }
}
