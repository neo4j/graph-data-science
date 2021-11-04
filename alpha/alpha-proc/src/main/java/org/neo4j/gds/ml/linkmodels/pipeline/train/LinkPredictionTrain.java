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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.LinkFeatureExtractor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionPredictor;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrain;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.ImmutableModelStats;
import org.neo4j.gds.ml.nodemodels.MetricData;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionTrain
    extends Algorithm<LinkPredictionTrain, Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo>> {

    public static final String MODEL_TYPE = "Link prediction pipeline";

    private final Graph trainGraph;
    private final Graph validationGraph;
    private final LinkPredictionPipeline pipeline;
    private final LinkPredictionTrainConfig trainConfig;
    private final AllocationTracker allocationTracker;

    public LinkPredictionTrain(
        Graph trainGraph,
        Graph validationGraph,
        LinkPredictionPipeline pipeline,
        LinkPredictionTrainConfig trainConfig,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.trainGraph = trainGraph;
        this.validationGraph = validationGraph;
        this.pipeline = pipeline;
        this.trainConfig = trainConfig;
        this.allocationTracker = AllocationTracker.empty();
    }

    static Task progressTask() {
        return Tasks.task(
            LinkPredictionTrain.class.getSimpleName(),
            Tasks.leaf("extract train features"),
            Tasks.leaf("select model"),
            Training.progressTask("train best model"),
            Tasks.leaf("compute train metrics"),
            Tasks.task(
                "evaluate on test data",
                Tasks.leaf("extract test features"),
                Tasks.leaf("compute test metrics")
            )
        );
    }

    @Override
    public Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> compute() {
        progressTracker.beginSubTask();

        progressTracker.beginSubTask("extract train features");
        var trainData = extractFeaturesAndTargets(trainGraph);
        var trainRelationshipIds = new ReadOnlyHugeLongIdentityArray(trainData.size());
        progressTracker.endSubTask("extract train features");

        progressTracker.beginSubTask("select model");
        var modelSelectResult = modelSelect(trainData, trainRelationshipIds);
        var bestParameters = modelSelectResult.bestParameters();
        progressTracker.endSubTask("select model");

        // train best model on the entire training graph
        progressTracker.beginSubTask("train best model");
        var modelData = trainModel(trainRelationshipIds, trainData, bestParameters, progressTracker);
        progressTracker.endSubTask("train best model");

        // evaluate the best model on the training and test graphs
        progressTracker.beginSubTask("compute train metrics");
        var outerTrainMetrics = computeTrainMetric(trainData, modelData, trainRelationshipIds, progressTracker);
        progressTracker.endSubTask("compute train metrics");

        progressTracker.beginSubTask("evaluate on test data");
        var testMetrics = computeTestMetric(modelData);
        progressTracker.endSubTask("evaluate on test data");

        var model = createModel(
            modelSelectResult,
            modelData,
            mergeMetrics(modelSelectResult, outerTrainMetrics, testMetrics)
        );

        progressTracker.endSubTask();

        return model;
    }

    private FeaturesAndTargets extractFeaturesAndTargets(Graph graph) {
        progressTracker.setVolume(graph.relationshipCount() * 2);
        var features = LinkFeatureExtractor.extractFeatures(
            graph,
            pipeline.featureSteps(),
            trainConfig.concurrency(),
            progressTracker
        );

        var targets = extractTargets(graph, features.size());

        return ImmutableFeaturesAndTargets.of(features, targets);
    }

    private HugeDoubleArray extractTargets(Graph graph, long numberOfTargets) {
        var globalTargets = HugeDoubleArray.newArray(numberOfTargets, allocationTracker);
        var relationshipIdx = new MutableLong();
        graph.forEachNode(nodeId -> {
            graph.forEachRelationship(nodeId, -10, (src, trg, weight) -> {
                if (weight == 0.0D || weight == 1.0D) {
                    globalTargets.set(relationshipIdx.getAndIncrement(), weight);
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Target should be either `1` or `0`. But got %f for relationship (%d, %d)",
                        weight,
                        src,
                        trg
                    ));
                }
                return true;
            });
            progressTracker.logProgress(graph.degree(nodeId));
            return true;
        });
        return globalTargets;
    }

    private LinkPredictionTrain.ModelSelectResult modelSelect(
        FeaturesAndTargets trainData,
        ReadOnlyHugeLongArray trainRelationshipIds
    ) {
        var validationSplits = trainValidationSplits(trainRelationshipIds, trainData.targets());

        var trainStats = initStatsMap();
        var validationStats = initStatsMap();

        var linkLogisticRegressionTrainConfigs = pipeline.trainingParameterSpace();
        progressTracker.setVolume(linkLogisticRegressionTrainConfigs.size());
        linkLogisticRegressionTrainConfigs.forEach(modelParams -> {
            var trainStatsBuilder = new ModelStatsBuilder(
                modelParams,
                pipeline.splitConfig().validationFolds()
            );
            var validationStatsBuilder = new ModelStatsBuilder(
                modelParams,
                pipeline.splitConfig().validationFolds()
            );
            for (TrainingExamplesSplit relSplit : validationSplits) {
                // train each model candidate on the train sets
                var trainSet = relSplit.trainSet();
                var validationSet = relSplit.testSet();
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

            progressTracker.logProgress();
        });

        // 5. pick the best-scoring model candidate, according to the main metric
        var mainMetric = trainConfig.metrics().get(0);
        var modelStats = validationStats.get(mainMetric);
        var winner = Collections.max(modelStats, COMPARE_AVERAGE);

        var bestConfig = winner.params();

        return LinkPredictionTrain.ModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    private Map<LinkMetric, Double> computeTestMetric(LinkLogisticRegressionData modelData) {
        progressTracker.beginSubTask("extract test features");
        var testData = extractFeaturesAndTargets(validationGraph);
        progressTracker.endSubTask("extract test features");

        progressTracker.beginSubTask("compute test metrics");
        var result = computeMetric(
            testData,
            modelData,
            new BatchQueue(testData.size()),
            progressTracker
        );
        progressTracker.endSubTask("compute test metrics");

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

    private List<TrainingExamplesSplit> trainValidationSplits(ReadOnlyHugeLongArray trainRelationshipIds, HugeDoubleArray actualTargets) {
        var splitter = new StratifiedKFoldSplitter(
            pipeline.splitConfig().validationFolds(),
            trainRelationshipIds,
            new ReadOnlyHugeDoubleToLongArrayWrapper(actualTargets),
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
        ReadOnlyHugeLongArray trainSet,
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
            terminationFlag,
            trainConfig.concurrency()
        );

        return llrTrain.compute();
    }

    private Map<LinkMetric, Double> computeTrainMetric(
        FeaturesAndTargets trainData,
        LinkLogisticRegressionData modelData,
        ReadOnlyHugeLongArray evaluationSet,
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

        evaluationQueue.parallelConsume(trainConfig.concurrency(), thread -> (batch) -> {
                for (Long relationshipIdx : batch.nodeIds()) {
                    double predictedProbability = predictor.predictedProbability(features.get(relationshipIdx));
                    boolean isEdge = targets.get(relationshipIdx) == 1.0D;

                    var signedProbability = isEdge ? predictedProbability : -1 * predictedProbability;
                    signedProbabilities.add(signedProbability);
                }

                progressTracker.logProgress(batch.size());
            },
            terminationFlag
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
            trainGraph.schema(),
            modelData,
            trainConfig,
            LinkPredictionModelInfo.of(
                modelSelectResult.bestParameters(),
                metrics,
                pipeline.copy()
            )
        );
    }

    @Override
    public LinkPredictionTrain me() {
        return this;
    }

    @Override
    public void release() {

    }
}
