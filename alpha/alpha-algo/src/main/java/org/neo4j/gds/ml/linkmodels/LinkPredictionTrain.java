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
package org.neo4j.gds.ml.linkmodels;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrain;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.util.ShuffleUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;
import static org.neo4j.gds.ml.util.ShuffleUtil.createRandomDataGenerator;

public class LinkPredictionTrain
    extends Algorithm<LinkPredictionTrain, Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo>> {

    public static final String MODEL_TYPE = "Link Prediction";

    private final Graph trainGraph;
    private final Graph testGraph;
    private final LinkPredictionTrainConfig config;
    private final AllocationTracker allocationTracker;
    private final List<FeatureExtractor> trainExtractors;
    private final List<FeatureExtractor> testExtractors;

    static MemoryEstimation estimateModelSelectResult(LinkPredictionTrainConfig config) {
        var numberOfParams = config.paramConfigs().size();
        var sizeOfOneModelStatsInBytes = sizeOfInstance(ImmutableModelStats.class);
        var sizeOfAllModelStatsInBytes = sizeOfOneModelStatsInBytes * numberOfParams;
        return MemoryEstimations.builder("model selection result")
            .fixed("instance", sizeOfInstance(ImmutableModelSelectResult.class))
            .fixed("model stats map train", sizeOfInstance(HashMap.class))
            .fixed("model stats list train", sizeOfInstance(ArrayList.class))
            .fixed("model stats map test", sizeOfInstance(HashMap.class))
            .fixed("model stats list test", sizeOfInstance(ArrayList.class))
            .fixed("model stats train", sizeOfAllModelStatsInBytes)
            .build();
    }

    public LinkPredictionTrain(
        Graph graph,
        LinkPredictionTrainConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.trainGraph = graph.relationshipTypeFilteredGraph(Set.of(config.trainRelationshipType()));
        this.testGraph = graph.relationshipTypeFilteredGraph(Set.of(config.testRelationshipType()));
        this.trainExtractors = FeatureExtraction.propertyExtractors(trainGraph, config.featureProperties());
        this.testExtractors = FeatureExtraction.propertyExtractors(testGraph, config.featureProperties());
        this.config = config;
        this.allocationTracker = AllocationTracker.empty();
    }

    @Override
    public Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> compute() {

        progressTracker.beginSubTask();
        // init and shuffle node ids
        var nodeIds = HugeLongArray.newArray(trainGraph.nodeCount(), allocationTracker);
        nodeIds.setAll(i -> i);
        ShuffleUtil.shuffleHugeLongArray(nodeIds, createRandomDataGenerator(config.randomSeed()));

        progressTracker.beginSubTask("ModelSelection");
        var modelSelectResult = modelSelect(nodeIds);
        progressTracker.endSubTask("ModelSelection");
        var bestParameters = modelSelectResult.bestParameters();

        // train best model on the entire training graph
        progressTracker.beginSubTask("Training");
        var modelData = trainModel(nodeIds, bestParameters, progressTracker);
        progressTracker.endSubTask("Training");

        // evaluate the best model on the training and test graphs
        progressTracker.beginSubTask("Evaluation");
        progressTracker.beginSubTask("Training");
        var outerTrainMetrics = computeMetric(trainGraph, nodeIds, predictor(modelData, trainExtractors),
            progressTracker
        );
        progressTracker.endSubTask("Training");

        progressTracker.beginSubTask("Testing");
        var testMetrics = computeMetric(testGraph, nodeIds, predictor(modelData, testExtractors), progressTracker);
        progressTracker.endSubTask("Testing");

        var metrics = mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics);
        progressTracker.endSubTask("Evaluation");
        progressTracker.endSubTask();

        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            trainGraph.schema(),
            modelData,
            config,
            LinkPredictionModelInfo.of(
                modelSelectResult.bestParameters(),
                metrics
            )
        );
    }

    public LinkLogisticRegressionPredictor predictor(LinkLogisticRegressionData modelData, List<FeatureExtractor> extractors) {
        return new LinkLogisticRegressionPredictor(modelData, extractors);
    }

    private Map<LinkMetric, MetricData<LinkLogisticRegressionTrainConfig>> mergeMetrics(
        ModelSelectResult modelSelectResult,
        Map<LinkMetric, Double> outerTrainMetrics,
        Map<LinkMetric, Double> testMetrics
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

    private ModelSelectResult modelSelect(HugeLongArray allNodeIds) {
        var nodeSplits = trainValidationSplits(allNodeIds);

        var trainStats = initStatsMap();
        var validationStats = initStatsMap();

        config.paramConfigs().forEach(modelParams -> {
            var trainStatsBuilder = new ModelStatsBuilder(
                modelParams,
                config.validationFolds()
            );
            var validationStatsBuilder = new ModelStatsBuilder(
                modelParams,
                config.validationFolds()
            );
            for (TrainingExamplesSplit nodeSplit : nodeSplits) {
                // 3. train each model candidate on the train sets
                var trainSet = nodeSplit.trainSet();
                var validationSet = nodeSplit.testSet();
                // we use a less fine grained progress logging for LP than for NC
                var modelData = trainModel(trainSet, modelParams, ProgressTracker.NULL_TRACKER);
                var predictor = predictor(modelData, trainExtractors);
                progressTracker.logProgress();

                // 4. evaluate each model candidate on the train and validation sets
                computeMetric(trainGraph, trainSet, predictor, ProgressTracker.NULL_TRACKER).forEach(trainStatsBuilder::update);
                computeMetric(trainGraph, validationSet, predictor, ProgressTracker.NULL_TRACKER).forEach(validationStatsBuilder::update);
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

    private List<TrainingExamplesSplit> trainValidationSplits(HugeLongArray allNodeIds) {
        var globalTargets = HugeLongArray.newArray(trainGraph.nodeCount(), allocationTracker);
        globalTargets.setAll(i -> 0L);
        var splitter = new StratifiedKFoldSplitter(config.validationFolds(), allNodeIds, globalTargets, config.randomSeed());
        return splitter.splits();
    }

    @ValueClass
    public interface ModelSelectResult {
        LinkLogisticRegressionTrainConfig bestParameters();

        Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> trainStats();
        Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> validationStats();

        static ModelSelectResult of(
            LinkLogisticRegressionTrainConfig bestConfig,
            Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> trainStats,
            Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
        }

    }

    private Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> initStatsMap() {
        var statsMap = new HashMap<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>>();
        statsMap.put(LinkMetric.AUCPR, new ArrayList<>());
        return statsMap;
    }

    private Map<LinkMetric, Double> computeMetric(
        Graph evaluationGraph,
        HugeLongArray evaluationSet,
        LinkLogisticRegressionPredictor predictor,
        ProgressTracker progressTracker
    ) {
        var signedProbabilities = SignedProbabilities.create(evaluationGraph.relationshipCount());

        progressTracker.setVolume(evaluationGraph.nodeCount());
        var queue = new HugeBatchQueue(evaluationSet);
        queue.parallelConsume(config.concurrency(), ignore -> new SignedProbabilitiesCollector(
                evaluationGraph.concurrentCopy(),
                predictor,
                signedProbabilities,
                progressTracker
            ),
            terminationFlag
        );

        return config.metrics().stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> metric.compute(signedProbabilities, config.negativeClassWeight())
        ));
    }

    private LinkLogisticRegressionData trainModel(
        HugeLongArray trainSet,
        LinkLogisticRegressionTrainConfig llrConfig,
        ProgressTracker progressTracker
    ) {
        var llrTrain = new LinkLogisticRegressionTrain(
            trainGraph,
            trainSet,
            trainExtractors,
            llrConfig,
            progressTracker,
            terminationFlag
        );

        return llrTrain.compute();
    }

    static class ModelStatsBuilder {
        private final Map<LinkMetric, Double> min;
        private final Map<LinkMetric, Double> max;
        private final Map<LinkMetric, Double> sum;
        private final LinkLogisticRegressionTrainConfig modelParams;
        private final int numberOfSplits;

        static long sizeInBytes() {
            return 3 * sizeOfInstance(HashMap.class) + 8;
        }

        ModelStatsBuilder(LinkLogisticRegressionTrainConfig modelParams, int numberOfSplits) {
            this.modelParams = modelParams;
            this.numberOfSplits = numberOfSplits;
            this.min = new HashMap<>();
            this.max = new HashMap<>();
            this.sum = new HashMap<>();
        }

        void update(LinkMetric metric, double value) {
            min.merge(metric, value, Math::min);
            max.merge(metric, value, Math::max);
            sum.merge(metric, value, Double::sum);
        }

        ModelStats<LinkLogisticRegressionTrainConfig> modelStats(LinkMetric metric) {
            return ImmutableModelStats.of(
                modelParams,
                sum.get(metric) / numberOfSplits,
                min.get(metric),
                max.get(metric)
            );
        }
    }

    @Override
    public LinkPredictionTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}
