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
import org.neo4j.gds.ml.nodemodels.logisticregression.MultiClassNLRTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRData;
import org.neo4j.gds.ml.nodemodels.multiclasslogisticregression.MultiClassNLRTrain;
import org.neo4j.gds.ml.splitting.FractionSplitter;
import org.neo4j.gds.ml.util.ShuffleUtil;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        var nodeIds = HugeLongArray.newArray(graph.nodeCount(), allocationTracker);
        ShuffleUtil.shuffleHugeLongArray(nodeIds, getRandomDataGenerator());
        var outerSplitter = new FractionSplitter();
        var outerSplit = outerSplitter.split(nodeIds, 1 - config.holdoutFraction());
        var candidates = concreteConfigs();
        var modelSelectResult = modelSelect(outerSplit.trainSet(), candidates, log);
        var bestConfig = modelSelectResult.bestConfig();
        // TODO: training may now only see outerSplitter.trainSet()
        var winnerTrain = new MultiClassNLRTrain(graph, outerSplit.trainSet(), bestConfig, log);
        var winnerModelData = winnerTrain.compute();
        // TODO: evaluate metrics on holdout and everything minus holdout
        Map<String, Double> testMetrics = config
            .metrics()
            .stream()
            .collect(Collectors.toMap(metric -> metric, metric -> 0.0));
        Map<String, Double> outerTrainMetrics = config
            .metrics()
            .stream()
            .collect(Collectors.toMap(metric -> metric, metric -> 0.0));

        // train on whole graph
        var reTrain = new MultiClassNLRTrain(graph, nodeIds, bestConfig, log);
        var retrainedModelData = reTrain.compute();
        var classes = sortedClasses(retrainedModelData);
        var metrics = mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics);

        var modelInfo = NodeClassificationModelInfo.of(classes, modelSelectResult.bestConfig().toMap(), metrics);


        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            graph.schema(),
            retrainedModelData,
            config,
            modelInfo.toMap()
        );
    }

    private RandomDataGenerator getRandomDataGenerator() {
        var random = new RandomDataGenerator();
        config.randomSeed().ifPresent(random::reSeed);
        return random;
    }

    private Map<String, MetricData> mergeMetrics(
        ModelSelectResult modelSelectResult,
        Map<String, Double> outerTrainMetrics,
        Map<String, Double> testMetrics
    ) {
        return config.metrics().stream().collect(Collectors.toMap(
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
        List<MultiClassNLRTrainConfig> concreteConfigs,
        Log log
    ) {
        // TODO: do real model selection

        var bestConfig = concreteConfigs.get(0);
        var emptyMetrics = config
            .metrics()
            .stream()
            .collect(Collectors.toMap(metric -> metric, metric -> List.<ConcreteModelStats>of()));
        return ModelSelectResult.of(bestConfig, emptyMetrics, emptyMetrics);
    }

    private List<MultiClassNLRTrainConfig> concreteConfigs() {
        return config.params().stream()
            .map(singleParams ->
                MultiClassNLRTrainConfig.of(
                    config.featureProperties(),
                    config.targetProperty(),
                    singleParams
                )
            ).collect(Collectors.toList());
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
        MultiClassNLRTrainConfig bestConfig();

        // key is metric
        Map<String, List<ConcreteModelStats>> trainStats();
        // key is metric
        Map<String, List<ConcreteModelStats>> validationStats();

        static ModelSelectResult of(
            MultiClassNLRTrainConfig bestConfig,
            Map<String, List<ConcreteModelStats>> trainStats,
            Map<String, List<ConcreteModelStats>> validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
        }

    }
}
