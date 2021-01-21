/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ml.BatchQueue;
import org.neo4j.gds.ml.nodemodels.logisticregression.MultiClassNLRTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRPredictor;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.NodeClassificationPredictConsumer;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeClassificationTrain
    extends Algorithm<NodeClassificationTrain, Model<MultiClassNLRData, NodeClassificationTrainConfig, NodeClassificationModelInfo>> {

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
    public Model<MultiClassNLRData, NodeClassificationTrainConfig, NodeClassificationModelInfo> compute() {
        // 1. Init and shuffle node ids
        var nodeIds = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        nodeIds.setAll(i -> i);
        ShuffleUtil.shuffleHugeLongArray(nodeIds, getRandomDataGenerator());

        // 2a. Outer split nodes into holdout + remaining
        var outerSplitter = new FractionSplitter();
        var outerSplit = outerSplitter.split(nodeIds, 1 - config.holdoutFraction());

        // model selection:
        var modelSelectResult = modelSelect(outerSplit.trainSet(), config.params());
        var bestParameters = modelSelectResult.bestParameters();

        // 6. retrain best model on remaining
        // TODO: training may now only see outerSplitter.trainSet()
        MultiClassNLRData winnerModelData = trainModel(bestParameters, outerSplit.trainSet());

        // 7. evaluate it on the holdout set
        // TODO: evaluate metrics on holdout and everything minus holdout
        var resolvedMetrics = modelSelectResult.validationStats().keySet();
        Map<Metric, Double> testMetrics = resolvedMetrics
            .stream()
            .collect(Collectors.toMap(metric -> metric, metric -> 0.0));
        Map<Metric, Double> outerTrainMetrics = resolvedMetrics
            .stream()
            .collect(Collectors.toMap(metric -> metric, metric -> 0.0));

        // 8. retrain that model on the full graph
        MultiClassNLRData retrainedModelData = trainModel(bestParameters, nodeIds);
        var classes = sortedClasses(retrainedModelData);
        var metrics = mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics);

        var modelInfo = NodeClassificationModelInfo.of(classes, modelSelectResult.bestParameters(), metrics);

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

    private List<Long> sortedClasses(MultiClassNLRData modelData) {
        return Arrays.stream(modelData.classIdMap().originalIds())
            .sorted()
            .boxed()
            .collect(Collectors.toList());
    }

    private ModelSelectResult modelSelect(
        HugeLongArray remainingSet,
        List<Map<String, Object>> concreteConfigs
    ) {
        var globalTargets = makeGlobalTargets();

        // 2b. Inner split: enumerate a number of train/validation splits of remaining
        var splitter = new StratifiedKFoldSplitter(config.validationFolds(), remainingSet, globalTargets);
        var splits = splitter.splits();

        var metrics = config
            .metrics()
            .stream()
            .map(Metric::valueOf)
            .collect(Collectors.toList());

        var trainStats = new HashMap<Metric, List<ConcreteModelStats>>();
        var validationStats = new HashMap<Metric, List<ConcreteModelStats>>();

        concreteConfigs.forEach(modelParams -> {
            var validationMin = new HashMap<Metric, Double>();
            var validationMax = new HashMap<Metric, Double>();
            var validationSum = new HashMap<Metric, Double>();
            for (NodeSplit split : splits) {
                // 3. train each model candidate on the train sets
                var trainSet = split.trainSet();
                var validationSet = split.testSet();
                MultiClassNLRData modelData = trainModel(modelParams, trainSet);

                // 4. evaluate each model candidate on the validation sets
                var predictor = new MultiClassNLRPredictor(modelData, config.featureProperties());
                var validationTargets = makeLocalTargets(validationSet);
                var validationTargetDebug = Arrays.toString(validationTargets.toArray());

                Map<Metric, Double> scores = computeMetrics(
                    validationTargets,
                    globalTargets,
                    metrics,
                    predictor,
                    validationSet
                );
                for (var score : scores.entrySet()) {
                    validationMin.merge(score.getKey(), score.getValue(), Math::min);
                    validationMax.merge(score.getKey(), score.getValue(), Math::max);
                    validationSum.merge(score.getKey(), score.getValue(), Double::sum);
                }
            }
            // insert the candidates metrics into validationStats
            metrics.forEach(metric -> {
                validationStats.compute(
                    metric,
                    (_m, concreteModelStats) -> {
                        if (concreteModelStats == null) {
                            concreteModelStats = new ArrayList<>();
                        }
                        var modelStats = ImmutableConcreteModelStats.of(
                            modelParams,
                            validationSum.get(metric) / splits.size(),
                            validationMin.get(metric),
                            validationMax.get(metric)
                        );
                        concreteModelStats.add(modelStats);
                        return concreteModelStats;
                    }
                );
                trainStats.compute(
                    metric,
                    (_m, concreteModelStats) -> {
                        if (concreteModelStats == null) {
                            concreteModelStats = new ArrayList<>();
                        }
                        return concreteModelStats;
                    }
                );
            });
        });

        // 5. pick the best-scoring model candidate, according to the main metric
        var mainMetric = metrics.get(0);
        var modelStats = validationStats.get(mainMetric);
        var winner = Collections.max(modelStats);

        var bestConfig = winner.params();
        return ModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    @NotNull
    private Map<Metric, Double> computeMetrics(
        HugeLongArray targets,
        HugeLongArray globalTargets,
        List<Metric> metrics,
        MultiClassNLRPredictor predictor,
        HugeLongArray validationSet
    ) {
        var predictedClasses = HugeLongArray.newArray(validationSet.size(), allocationTracker);

        // consume from queue which contains local nodeIds, i.e. indices into validationSet
        // the consumer internally remaps to original nodeIds before prediction
        var consumer = new NodeClassificationPredictConsumer(
            graph,
            validationSet::get,
            predictor,
            null,
            predictedClasses,
            progressLogger
        );

        var queue = new BatchQueue(validationSet.size());
        queue.parallelConsume(consumer, config.concurrency());

        var scores = metrics.stream().collect(Collectors.toMap(
            metric -> metric,
            metric -> metric.compute(targets, predictedClasses, globalTargets)
        ));
        return scores;
    }

    private MultiClassNLRData trainModel(Map<String, Object> modelParams, HugeLongArray trainSet) {
        var nlrConfig = MultiClassNLRTrainConfig.of(
            config.featureProperties(),
            config.targetProperty(),
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
        Map<Metric, List<ConcreteModelStats>> trainStats();
        // key is metric
        Map<Metric, List<ConcreteModelStats>> validationStats();

        static ModelSelectResult of(
            Map<String, Object> bestConfig,
            Map<Metric, List<ConcreteModelStats>> trainStats,
            Map<Metric, List<ConcreteModelStats>> validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
        }

    }
}
