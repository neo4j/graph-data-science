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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrain;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.splitting.NodeSplit;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionTrain
    extends Algorithm<LinkPredictionTrain, Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo>> {

    public static final String MODEL_TYPE = "Link prediction pipeline";

    private final GraphStore graphStore;
    private final LinkPredictionTrainConfig trainConfig;
    private final PipelineExecutor pipelineExecutor;
    private final TrainingPipeline pipeline;
    private final AllocationTracker allocationTracker;

    public LinkPredictionTrain(
        GraphStore graphStore,
        LinkPredictionTrainConfig trainConfig,
        TrainingPipeline pipeline,
        PipelineExecutor pipelineExecutor,
        ProgressTracker progressTracker
    ) {
        this.graphStore = graphStore;
        this.trainConfig = trainConfig;
        this.pipelineExecutor = pipelineExecutor;
        this.pipeline = pipeline;
        this.progressTracker = progressTracker;
        this.allocationTracker = AllocationTracker.empty();
    }

    @Override
    public Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> compute() {
        List<String> relationshipTypes = trainConfig
            .internalRelationshipTypes(graphStore)
            .stream()
            .map(RelationshipType::name)
            .collect(Collectors.toList());

        pipelineExecutor.splitRelationships(graphStore, relationshipTypes, trainConfig.nodeLabels(), trainConfig.randomSeed());

        pipelineExecutor.executeNodePropertySteps(
            trainConfig.nodeLabelIdentifiers(graphStore),
            RelationshipType.of(pipeline.splitConfig().featureInputRelationshipType())
        );

        var trainData = extractFeaturesAndTargets(pipeline.splitConfig().trainRelationshipType());
        var trainRelationshipIds = HugeLongArray.newArray(trainData.size(), allocationTracker);
        trainRelationshipIds.setAll(i -> i);

        var modelSelectResult = modelSelect(trainData, trainRelationshipIds);
        var bestParameters = modelSelectResult.bestParameters();

        // train best model on the entire training graph
        var modelData = trainModel(trainRelationshipIds, trainData, bestParameters, progressTracker);

        // evaluate the best model on the training and test graphs
        var outerTrainMetrics = computeTrainMetric(trainData, modelData, trainRelationshipIds, progressTracker);
        var testMetrics = computeTestMetric(modelData);

        cleanUpGraphStore();

        return createModel(
            modelSelectResult,
            modelData,
            mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics)
        );
    }

    FeaturesAndTargets extractFeaturesAndTargets(String relationshipType) {
        var features = pipelineExecutor.computeFeatures(
            trainConfig.nodeLabelIdentifiers(graphStore),
            RelationshipType.of(relationshipType),
            trainConfig.concurrency()
        );
        var targets = extractTargets(features.size(), relationshipType);

        return ImmutableFeaturesAndTargets.of(features, targets);
    }

    public HugeDoubleArray extractTargets(long numberOfTargets, String relationshipType) {
        var globalTargets = HugeDoubleArray.newArray(numberOfTargets, allocationTracker);
        var trainGraph = graphStore.getGraph(RelationshipType.of(relationshipType), Optional.of("label"));
        var relationshipIdx = new MutableLong();
        trainGraph.forEachNode(nodeId -> {
            trainGraph.forEachRelationship(nodeId, -10, (src, trg, weight) -> {
                if (weight == 0.0D || weight == 1.0D) {
                    globalTargets.set(relationshipIdx.getAndIncrement(), weight);
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Target should be either `1` or `0`. But got %d for relationship (%d, %d)",
                        weight,
                        src,
                        trg
                    ));
                }
                return true;
            });
            return true;
        });
        return globalTargets;
    }

    private LinkPredictionTrain.ModelSelectResult modelSelect(
        FeaturesAndTargets trainData,
        HugeLongArray trainRelationshipIds
    ) {

        var validationSplits = trainValidationSplits(trainRelationshipIds, trainData.targets());

        var trainStats = initStatsMap();
        var validationStats = initStatsMap();

        pipeline.parameterConfigs(trainConfig.concurrency()).forEach(modelParams -> {
            var trainStatsBuilder = new ModelStatsBuilder(
                modelParams,
                pipeline.splitConfig().validationFolds()
            );
            var validationStatsBuilder = new ModelStatsBuilder(
                modelParams,
                pipeline.splitConfig().validationFolds()
            );
            for (NodeSplit split : validationSplits) {
                // train each model candidate on the train sets
                var trainSet = split.trainSet();
                var validationSet = split.testSet();
                // the below calls intentionally suppress progress logging of individual models
                var modelData = trainModel(trainSet, trainData, modelParams, ProgressTracker.NULL_TRACKER);

                // evaluate each model candidate on the train and validation sets
                computeTrainMetric(trainData, modelData, trainSet, ProgressTracker.NULL_TRACKER)
                    .forEach(trainStatsBuilder::update);
                computeTrainMetric(trainData, modelData, validationSet, ProgressTracker.NULL_TRACKER)
                    .forEach(validationStatsBuilder::update);
            }

            // insert the candidates metrics into trainStats and validationStats
            trainConfig.metrics().forEach(metric -> {
                validationStats.get(metric).add(validationStatsBuilder.modelStats(metric));
                trainStats.get(metric).add(trainStatsBuilder.modelStats(metric));
            });
        });

        // 5. pick the best-scoring model candidate, according to the main metric
        var mainMetric = trainConfig.metrics().get(0);
        var modelStats = validationStats.get(mainMetric);
        var winner = Collections.max(modelStats, COMPARE_AVERAGE);

        var bestConfig = winner.params();

        return LinkPredictionTrain.ModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    private Map<LinkMetric, Double> computeTestMetric(LinkLogisticRegressionData modelData) {
        var testData = extractFeaturesAndTargets(pipeline.splitConfig().testRelationshipType());

        var result = computeMetric(
            testData,
            modelData,
            new BatchQueue(testData.size()),
            progressTracker
        );

        return result;
    }

    private Map<LinkMetric, MetricData<LinkLogisticRegressionTrainConfig>> mergeMetrics(
        LinkPredictionTrain.ModelSelectResult modelSelectResult,
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


    private List<NodeSplit> trainValidationSplits(HugeLongArray trainRelationshipIds, HugeDoubleArray actualTargets) {
        var globalTargets = HugeLongArray.newArray(trainRelationshipIds.size(), allocationTracker);
        globalTargets.setAll(i -> (long) actualTargets.get(i));
        var splitter = new StratifiedKFoldSplitter(
            pipeline.splitConfig().validationFolds(),
            trainRelationshipIds,
            globalTargets,
            trainConfig.randomSeed()
        );
        return splitter.splits();
    }

    private Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> initStatsMap() {
        var statsMap = new HashMap<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>>();
        statsMap.put(LinkMetric.AUCPR, new ArrayList<>());
        return statsMap;
    }

    @ValueClass
    public interface ModelSelectResult {

        LinkLogisticRegressionTrainConfig bestParameters();

        Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> trainStats();

        Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> validationStats();

        static LinkPredictionTrain.ModelSelectResult of(
            LinkLogisticRegressionTrainConfig bestConfig,
            Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> trainStats,
            Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> validationStats
        ) {
            return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
        }
    }


    private LinkLogisticRegressionData trainModel(
        HugeLongArray trainSet,
        FeaturesAndTargets trainData,
        LinkLogisticRegressionTrainConfig llrConfig,
        ProgressTracker progressTracker
    ) {
        var llrTrain = new LinkLogisticRegressionTrain(
            trainSet,
            trainData.features(),
            trainData.targets(),
            llrConfig,
            progressTracker,
            terminationFlag
        );
        var model = llrTrain.compute();

        return model;
    }

    private Map<LinkMetric, Double> computeTrainMetric(
        FeaturesAndTargets trainData,
        LinkLogisticRegressionData modelData,
        HugeLongArray evaluationSet,
        ProgressTracker progressTracker
    ) {
        return computeMetric(trainData, modelData, new HugeBatchQueue(evaluationSet), progressTracker);
    }

    private Map<LinkMetric, Double> computeMetric(
        FeaturesAndTargets inputData,
        LinkLogisticRegressionData modelData,
        BatchQueue evaluationQueue,
        ProgressTracker progressTracker
    ) {
        progressTracker.setVolume(inputData.size());

        var predictor = new LinkLogisticRegressionPredictor(modelData);
        var signedProbabilities = SignedProbabilities.create(inputData.size());
        var targets = inputData.targets();
        var features = inputData.features();

        evaluationQueue.parallelConsume(trainConfig.concurrency(), thread ->
            (batch) -> {
                for (Long relationshipIdx : batch.nodeIds()) {
                    double predictedProbability = predictor.predictedProbability(features.get(relationshipIdx));
                    boolean isEdge = targets.get(relationshipIdx) == 1.0D;

                    var signedProbability = isEdge ? predictedProbability : -1 * predictedProbability;
                    signedProbabilities.add(signedProbability);
                }
            }
        );

        return trainConfig.metrics().stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> metric.compute(signedProbabilities, trainConfig.negativeClassWeight())
        ));
    }

    static class ModelStatsBuilder {
        private final Map<LinkMetric, Double> min;
        private final Map<LinkMetric, Double> max;
        private final Map<LinkMetric, Double> sum;
        private final LinkLogisticRegressionTrainConfig modelParams;
        private final int numberOfSplits;

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

    private Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> createModel(
        ModelSelectResult modelSelectResult,
        LinkLogisticRegressionData modelData,
        Map<LinkMetric, MetricData<LinkLogisticRegressionTrainConfig>> metrics
    ) {
        return Model.of(
            trainConfig.username(),
            trainConfig.modelName(),
            MODEL_TYPE,
            graphStore.schema(),
            modelData,
            trainConfig,
            LinkPredictionModelInfo.of(
                modelSelectResult.bestParameters(),
                metrics,
                pipeline.copy()
            )
        );
    }

    private void cleanUpGraphStore() {
        LinkPredictionSplitConfig splitConfig = pipeline.splitConfig();
        var trainRelTypes = List.of(
            splitConfig.trainRelationshipType(),
            splitConfig.testRelationshipType(),
            splitConfig.featureInputRelationshipType()
        );
        trainRelTypes.forEach(relType -> graphStore.deleteRelationships(RelationshipType.of(relType)));

        pipelineExecutor.removeNodeProperties(graphStore, trainConfig.nodeLabelIdentifiers(graphStore));
    }

    @Override
    public LinkPredictionTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}
