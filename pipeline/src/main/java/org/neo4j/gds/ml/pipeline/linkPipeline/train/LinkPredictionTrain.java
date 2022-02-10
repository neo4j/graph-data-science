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
package org.neo4j.gds.ml.pipeline.linkPipeline.train;

import org.immutables.value.Value;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.collections.ReadOnlyHugeLongIdentityArray;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.Training;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.linkmodels.metrics.LinkMetric;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrain;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.BestMetricData;
import org.neo4j.gds.ml.nodemodels.BestModelStats;
import org.neo4j.gds.ml.nodemodels.ModelStats;
import org.neo4j.gds.ml.nodemodels.StatsMap;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkFeaturesAndTargetsExtractor.extractFeaturesAndTargets;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionEvaluationMetricComputer.computeMetric;

public class LinkPredictionTrain extends Algorithm<LinkPredictionTrainResult> {

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

    public static Task progressTask() {
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
    public LinkPredictionTrainResult compute() {
        progressTracker.beginSubTask();

        progressTracker.beginSubTask("extract train features");
        var trainData = extractFeaturesAndTargets(
            trainGraph,
            pipeline.featureSteps(),
            trainConfig.concurrency(),
            progressTracker,
            allocationTracker
        );
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
            bestParameters,
            modelData,
            combineBestParameterMetrics(modelSelectResult, outerTrainMetrics, testMetrics)
        );

        progressTracker.endSubTask();

        return ImmutableLinkPredictionTrainResult.of(model, modelSelectResult);
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
            var trainStatsBuilder = new LinkModelStatsBuilder(modelParams, pipeline.splitConfig().validationFolds());
            var validationStatsBuilder = new LinkModelStatsBuilder(modelParams, pipeline.splitConfig().validationFolds());
            for (TrainingExamplesSplit relSplit : validationSplits) {
                // train each model candidate on the train sets
                var trainSet = relSplit.trainSet();
                var validationSet = relSplit.testSet();
                // the below calls intentionally suppress progress logging of individual models
                var modelData = trainModel(ReadOnlyHugeLongArray.of(trainSet), trainData, modelParams, ProgressTracker.NULL_TRACKER);

                // evaluate each model candidate on the train and validation sets
                computeTrainMetric(trainData, modelData, ReadOnlyHugeLongArray.of(trainSet), ProgressTracker.NULL_TRACKER)
                    .forEach(trainStatsBuilder::update);
                computeTrainMetric(trainData, modelData, ReadOnlyHugeLongArray.of(validationSet), ProgressTracker.NULL_TRACKER)
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

        return ModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    private Map<LinkMetric, Double> computeTestMetric(LinkLogisticRegressionData modelData) {
        progressTracker.beginSubTask("extract test features");
        var testData = extractFeaturesAndTargets(
            validationGraph,
            pipeline.featureSteps(),
            trainConfig.concurrency(),
            progressTracker,
            allocationTracker
        );
        progressTracker.endSubTask("extract test features");

        progressTracker.beginSubTask("compute test metrics");
        var result = computeMetric(
            testData,
            modelData,
            new BatchQueue(testData.size()),
            trainConfig,
            progressTracker,
            terminationFlag
        );
        progressTracker.endSubTask("compute test metrics");

        return result;
    }

    private static Map<LinkMetric, BestMetricData> combineBestParameterMetrics(
        LinkPredictionTrain.ModelSelectResult modelSelectResult,
        Map<LinkMetric, Double> outerTrainMetrics,
        Map<LinkMetric, Double> testMetrics
    ) {
        Set<LinkMetric> metrics = modelSelectResult.validationStats().keySet();

        return metrics.stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> BestMetricData.of(
                findBestModelStats(modelSelectResult.trainStats().get(metric), modelSelectResult.bestParameters()),
                findBestModelStats(modelSelectResult.validationStats().get(metric), modelSelectResult.bestParameters()),
                outerTrainMetrics.get(metric),
                testMetrics.get(metric)
            )
        ));
    }

    private static BestModelStats findBestModelStats(
        List<ModelStats<LinkLogisticRegressionTrainConfig>> metricStatsForModels,
        LinkLogisticRegressionTrainConfig bestParams
    ) {
        return metricStatsForModels.stream()
            .filter(metricStatsForModel -> metricStatsForModel.params() == bestParams)
            .findFirst()
            .map(BestModelStats::of)
            .orElseThrow();
    }

    private List<TrainingExamplesSplit> trainValidationSplits(
        ReadOnlyHugeLongArray trainRelationshipIds,
        HugeDoubleArray actualTargets
    ) {
        var splitter = new StratifiedKFoldSplitter(
            pipeline.splitConfig().validationFolds(),
            trainRelationshipIds,
            new ReadOnlyHugeDoubleToLongArrayWrapper(actualTargets),
            trainConfig.randomSeed()
        );
        return splitter.splits();
    }

    private static Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>> initStatsMap() {
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

        @Value.Derived
        default Map<String, Object> toMap() {
            Function<Map<LinkMetric, List<ModelStats<LinkLogisticRegressionTrainConfig>>>, Map<String, Object>> statsConverter = stats ->
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

    private LinkLogisticRegressionData trainModel(
        ReadOnlyHugeLongArray trainSet,
        FeaturesAndTargets trainData,
        LinkLogisticRegressionTrainConfig llrConfig,
        ProgressTracker progressTracker
    ) {
        return new LinkLogisticRegressionTrain(
            trainSet,
            trainData.features(),
            trainData.targets(),
            llrConfig,
            progressTracker,
            terminationFlag,
            trainConfig.concurrency()
        ).compute();
    }

    private Map<LinkMetric, Double> computeTrainMetric(
        FeaturesAndTargets trainData,
        LinkLogisticRegressionData modelData,
        ReadOnlyHugeLongArray evaluationSet,
        ProgressTracker progressTracker
    ) {
        return computeMetric(
            trainData,
            modelData,
            new HugeBatchQueue(evaluationSet),
            trainConfig,
            progressTracker,
            terminationFlag
        );
    }

    private Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> createModel(
        LinkLogisticRegressionTrainConfig bestParameters,
        LinkLogisticRegressionData modelData,
        Map<LinkMetric, BestMetricData> winnerMetrics
    ) {
        return Model.of(
            trainConfig.username(),
            trainConfig.modelName(),
            MODEL_TYPE,
            trainGraph.schema(),
            modelData,
            trainConfig,
            LinkPredictionModelInfo.of(
                bestParameters,
                winnerMetrics,
                pipeline.copy()
            )
        );
    }

    @Override
    public void release() {

    }

    public static MemoryEstimation estimate(
        LinkPredictionPipeline pipeline,
        LinkPredictionTrainConfig trainConfig
    ) {
        var splitConfig = pipeline.splitConfig();

        var builder = MemoryEstimations.builder(LinkPredictionTrain.class);

        var fudgedLinkFeatureDim = MemoryRange.of(10, 500);

        int numberOfMetrics = trainConfig.metrics().size();
        return builder
            // After the training, the training features and targets are no longer accessed.
            // As the lifetimes of training and test data is non-overlapping, we assume the max is sufficient.
            .max("Features and targets", List.of(
                LinkFeaturesAndTargetsExtractor.estimate(
                    fudgedLinkFeatureDim,
                    totalRelCount -> splitConfig.expectedSetSizes(totalRelCount).trainSize(),
                    "Train"
                ),
                LinkFeaturesAndTargetsExtractor.estimate(
                    fudgedLinkFeatureDim,
                    totalRelCount -> splitConfig.expectedSetSizes(totalRelCount).testSize(),
                    "Test"
                )
            ))
            .add(estimateModelSelection(pipeline, fudgedLinkFeatureDim, numberOfMetrics))
            // we do not consider the training of the best model on the outer train set as the memory estimation is at most the maximum of the model training during the model selection
            // this assumes the training is independent of the relationship set size
            .add("Outer train stats map", StatsMap.memoryEstimation(numberOfMetrics, 1, 1))
            .add("Test stats map", StatsMap.memoryEstimation(numberOfMetrics, 1, 1))
            .fixed("Best model stats", numberOfMetrics * BestMetricData.estimateMemory())
            .build();
    }

    private static MemoryEstimation estimateModelSelection(
        LinkPredictionPipeline pipeline,
        MemoryRange linkFeatureDimension,
        int numberOfMetrics
    ) {
        var splitConfig = pipeline.splitConfig();
        var maxEstimationOverModelCandidates = maxEstimation(
            "Max over model candidates",
            pipeline.trainingParameterSpace().stream()
                .map(llrConfig -> MemoryEstimations.builder("Train and evaluate model")
                    .fixed("Stats map builder train", LinkModelStatsBuilder.sizeInBytes(numberOfMetrics))
                    .fixed("Stats map builder validation", LinkModelStatsBuilder.sizeInBytes(numberOfMetrics))
                    .max("Train model and compute train metrics", List.of(
                            LinkLogisticRegressionTrain.estimate(llrConfig, linkFeatureDimension),
                            estimateComputeTrainMetrics(pipeline.splitConfig())
                        )
                    ).build()
                ).collect(Collectors.toList())
        );

        return MemoryEstimations.builder("model selection")
            .add(
                "Cross-Validation splitting",
                StratifiedKFoldSplitter.memoryEstimation(
                    splitConfig.validationFolds(),
                    dim -> splitConfig.expectedSetSizes(dim.relCountUpperBound()).trainSize()
                )
            )
            .add(maxEstimationOverModelCandidates)
            .add(
                "Inner train stats map",
                StatsMap.memoryEstimation(numberOfMetrics, pipeline.trainingParameterSpace().size(), 1)
            )
            .add(
                "Validation stats map",
                StatsMap.memoryEstimation(numberOfMetrics, pipeline.trainingParameterSpace().size(), 1)
            )
            .build();
    }

    private static MemoryEstimation estimateComputeTrainMetrics(LinkPredictionSplitConfig splitConfig) {
        return MemoryEstimations
            .builder("Compute train metrics")
            .perGraphDimension(
                "Sorted probabilities",
                (dim, threads) -> {
                    long trainSetSize = splitConfig
                        .expectedSetSizes(dim.relCountUpperBound())
                        .trainSize();
                    return LinkPredictionEvaluationMetricComputer.estimate(trainSetSize);
                }
            ).build();
    }
}
