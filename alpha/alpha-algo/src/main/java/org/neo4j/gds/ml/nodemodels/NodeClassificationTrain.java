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
import org.neo4j.gds.ml.batch.BatchQueue;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRPredictor;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.splitting.NodeSplit;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;

public class NodeClassificationTrain
    extends Algorithm<NodeClassificationTrain, Model<MultiClassNLRData, NodeClassificationTrainConfig>> {

    public static final String MODEL_TYPE = "multiClassNodeLogisticRegression";

    private final Graph graph;
    private final NodeClassificationTrainConfig config;
    private final Log log;
    private final AllocationTracker allocationTracker;

    public NodeClassificationTrain(
        Graph graph,
        NodeClassificationTrainConfig config,
        Log log
    ) {
        this.graph = graph;
        this.config = config;
        this.log = log;
        this.allocationTracker = AllocationTracker.empty();
    }

    @Override
    public Model<MultiClassNLRData, NodeClassificationTrainConfig> compute() {
        // 1. Init and shuffle node ids
        var nodeIds = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        nodeIds.setAll(i -> i);
        ShuffleUtil.shuffleHugeLongArray(nodeIds, getRandomDataGenerator());

        // 2a. Outer split nodes into holdout + remaining
        var outerSplitter = new FractionSplitter();
        var outerSplit = outerSplitter.split(nodeIds, 1 - config.holdoutFraction());

        // model selection:
        var globalTargets = makeGlobalTargets();
        var modelSelectResult = modelSelect(outerSplit.trainSet(), globalTargets);
        var bestParameters = modelSelectResult.bestParameters();

        // 6. train best model on remaining
        MultiClassNLRData winnerModelData = trainModel(outerSplit.trainSet(), bestParameters);

        // 7. evaluate it on the holdout set and outer training set
        var testMetrics = computeMetrics(globalTargets, outerSplit.testSet(), winnerModelData);
        var outerTrainMetrics = computeMetrics(globalTargets, outerSplit.trainSet(), winnerModelData);

        // we are done with all metrics!
        var metrics = mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics);

        // 8. retrain that model on the full graph
        MultiClassNLRData retrainedModelData = trainModel(nodeIds, bestParameters);

        var modelInfo = NodeClassificationModelInfo.of(
            retrainedModelData.classIdMap().originalIdsList(),
            modelSelectResult.bestParameters(),
            metrics
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

    private RandomDataGenerator getRandomDataGenerator() {
        var random = new RandomDataGenerator();
        config.randomSeed().ifPresent(random::reSeed);
        return random;
    }

    private Map<Metric, MetricData> mergeMetrics(
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

    private ModelSelectResult modelSelect(HugeLongArray remainingSet, HugeLongArray globalTargets) {

        // 2b. Inner split: enumerate a number of train/validation splits of remaining
        var splitter = new StratifiedKFoldSplitter(config.validationFolds(), remainingSet, globalTargets);
        var splits = splitter.splits();

        var trainStats = initStatsMap();
        var validationStats = initStatsMap();

        config.params().forEach(modelParams -> {
            var validationStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, splits.size());
            for (NodeSplit split : splits) {
                // 3. train each model candidate on the train sets
                var trainSet = split.trainSet();
                var validationSet = split.testSet();
                var modelData = trainModel(trainSet, modelParams);

                // 4. evaluate each model candidate on the train and validation sets
                computeMetrics(globalTargets, validationSet, modelData).forEach(validationStatsBuilder::update);
                computeMetrics(globalTargets, trainSet, modelData).forEach(trainStatsBuilder::update);
            }
            // insert the candidates metrics into trainStats and validationStats
            config.metrics().forEach(metric -> {
                validationStats.get(metric).add(validationStatsBuilder.modelStats(metric));
                trainStats.get(metric).add(trainStatsBuilder.modelStats(metric));
            });
        });

        // 5. pick the best-scoring model candidate, according to the main metric
        var mainMetric = config.metrics().get(0);
        var modelStats = validationStats.get(mainMetric);
        var winner = Collections.max(modelStats, COMPARE_AVERAGE);

        var bestConfig = winner.params();
        return ModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    private Map<Metric, List<ModelStats>> initStatsMap() {
        var statsMap = new HashMap<Metric, List<ModelStats>>();
        config.metrics().forEach(metric -> statsMap.put(metric, new ArrayList<>()));
        return statsMap;
    }

    private Map<Metric, Double> computeMetrics(
        HugeLongArray globalTargets,
        HugeLongArray evaluationSet,
        MultiClassNLRData modelData
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
            progressLogger
        );

        var queue = new BatchQueue(evaluationSet.size());
        queue.parallelConsume(consumer, config.concurrency());

        return config.metrics().stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> metric.compute(localTargets, predictedClasses, globalTargets)
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
        var train = new MultiClassNLRTrain(graph, trainSet, nlrConfig, log);
        return train.compute();
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
