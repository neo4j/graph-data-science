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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.core.ReadOnlyHugeLongIdentityArray;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.batch.HugeBatchQueue;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.BestMetricData;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.metrics.SignedProbabilities;
import org.neo4j.gds.ml.metrics.StatsMap;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.pipeline.TrainingStatistics;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkFeaturesAndLabelsExtractor.extractFeaturesAndLabels;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionTrain extends Algorithm<LinkPredictionTrainResult> {

    public static final String MODEL_TYPE = "LinkPrediction";

    private final Graph trainGraph;
    private final Graph validationGraph;
    private final LinkPredictionTrainingPipeline pipeline;
    private final LinkPredictionTrainConfig config;
    private final LocalIdMap classIdMap;

    public static LocalIdMap makeClassIdMap() {
        return LocalIdMap.of((long) EdgeSplitter.NEGATIVE, (long) EdgeSplitter.POSITIVE);
    }

    public LinkPredictionTrain(
        Graph trainGraph,
        Graph validationGraph,
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.trainGraph = trainGraph;
        this.validationGraph = validationGraph;
        this.pipeline = pipeline;
        this.config = config;
        this.classIdMap = makeClassIdMap();
    }

    public static List<Task> progressTasks(int validationFolds, int numberOfModelSelectionTrials) {
        return List.of(
            Tasks.leaf("Extract train features"),
            Tasks.iterativeFixed(
                "Select best model",
                () -> List.of(Tasks.leaf("Trial", validationFolds)),
                numberOfModelSelectionTrials
            ),
            ClassifierTrainer.progressTask("Train best model"),
            Tasks.leaf("Compute train metrics"),
            Tasks.task(
                "Evaluate on test data",
                Tasks.leaf("Extract test features"),
                Tasks.leaf("Compute test metrics")
            )
        );
    }

    @Override
    public LinkPredictionTrainResult compute() {

        progressTracker.beginSubTask("Extract train features");
        var trainData = extractFeaturesAndLabels(
            trainGraph,
            pipeline.featureSteps(),
            config.concurrency(),
            progressTracker
        );
        var trainRelationshipIds = new ReadOnlyHugeLongIdentityArray(trainData.size());
        progressTracker.endSubTask("Extract train features");

        progressTracker.beginSubTask("Select best model");

        var trainingStatistics = new TrainingStatistics(List.copyOf(config.metrics()));

        modelSelect(trainData, trainRelationshipIds, trainingStatistics);
        progressTracker.endSubTask("Select best model");

        // train best model on the entire training graph
        progressTracker.beginSubTask("Train best model");
        var classifier = trainModel(
            trainData,
            trainRelationshipIds,
            trainingStatistics.bestParameters(),
            progressTracker
        );
        progressTracker.endSubTask("Train best model");

        // evaluate the best model on the training and test graphs
        progressTracker.beginSubTask("Compute train metrics");
        computeTrainMetric(
            trainData,
            classifier,
            trainRelationshipIds,
            trainingStatistics::addOuterTrainScore
        );
        progressTracker.endSubTask("Compute train metrics");

        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logMessage(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));

        progressTracker.beginSubTask("Evaluate on test data");
        computeTestMetric(classifier, trainingStatistics);
        progressTracker.endSubTask("Evaluate on test data");

        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logMessage(formatWithLocale("Final model metrics on test set: %s", testMetrics));

        var model = createModel(
            trainingStatistics.bestParameters(),
            classifier.data(),
            trainingStatistics.metricsForWinningModel()
        );

        return LinkPredictionTrainResult.of(model, trainingStatistics);
    }

    @NotNull
    private Classifier trainModel(
        FeaturesAndLabels featureAndLabels,
        ReadOnlyHugeLongArray trainSet,
        TrainerConfig trainerConfig,
        ProgressTracker customProgressTracker
    ) {
        return ClassifierTrainerFactory.create(
            trainerConfig,
            classIdMap,
            terminationFlag,
            customProgressTracker,
            config.concurrency(),
            config.randomSeed(),
            true
        ).train(featureAndLabels.features(), featureAndLabels.labels(), trainSet);
    }

    private void modelSelect(
        FeaturesAndLabels trainData,
        ReadOnlyHugeLongArray trainRelationshipIds,
        TrainingStatistics trainingStatistics
    ) {
        var validationSplits = trainValidationSplits(trainRelationshipIds, trainData.labels());

        var hyperParameterOptimizer = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.numberOfModelSelectionTrials(),
            config.randomSeed()
        );

        while (hyperParameterOptimizer.hasNext()) {
            progressTracker.beginSubTask();
            var modelParams = hyperParameterOptimizer.next();
            progressTracker.logMessage(formatWithLocale("Parameters: %s", modelParams.toMap()));
            var trainStatsBuilder = new ModelStatsBuilder(modelParams, pipeline.splitConfig().validationFolds());
            var validationStatsBuilder = new ModelStatsBuilder(
                modelParams,
                pipeline.splitConfig().validationFolds()
            );
            for (TrainingExamplesSplit relSplit : validationSplits) {
                // train each model candidate on the train sets
                var trainSet = relSplit.trainSet();
                var validationSet = relSplit.testSet();
                // the below calls intentionally suppress progress logging of individual models
                var classifier = trainModel(
                    trainData,
                    ReadOnlyHugeLongArray.of(trainSet),
                    modelParams,
                    ProgressTracker.NULL_TRACKER
                );

                // evaluate each model candidate on the train and validation sets
                computeTrainMetric(
                    trainData,
                    classifier,
                    ReadOnlyHugeLongArray.of(trainSet),
                    trainStatsBuilder::update
                );
                computeTrainMetric(
                    trainData,
                    classifier,
                    ReadOnlyHugeLongArray.of(validationSet),
                    validationStatsBuilder::update
                );
                progressTracker.logProgress();
            }

            // insert the candidates' metrics into trainStats and validationStats
            config.metrics().forEach(metric -> {
                trainingStatistics.addValidationStats(metric, validationStatsBuilder.build(metric));
                trainingStatistics.addTrainStats(metric, trainStatsBuilder.build(metric));
            });
            var validationStats = trainingStatistics.findModelValidationAvg(modelParams);
            var trainStats = trainingStatistics.findModelTrainAvg(modelParams);
            double mainMetric = validationStats.get(config.metrics().get(0));

            progressTracker.logMessage(formatWithLocale("Main validation metric: %.4f", mainMetric));
            progressTracker.logMessage(formatWithLocale("Validation metrics: %s", validationStats));
            progressTracker.logMessage(formatWithLocale("Training metrics: %s", trainStats));

            progressTracker.endSubTask();
        }

        int bestTrial = trainingStatistics.getBestTrialIdx() + 1;
        double bestTrialScore = trainingStatistics.getBestTrialScore();
        progressTracker.logMessage(formatWithLocale(
            "Best trial was Trial %d with main validation metric %.4f",
            bestTrial,
            bestTrialScore
        ));
    }

    private void computeTestMetric(Classifier classifier, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask("Extract test features");
        var testData = extractFeaturesAndLabels(
            validationGraph,
            pipeline.featureSteps(),
            config.concurrency(),
            progressTracker
        );
        progressTracker.endSubTask("Extract test features");

        progressTracker.beginSubTask("Compute test metrics");
        var signedProbabilities = SignedProbabilities.computeFromLabeledData(
            testData.features(),
            testData.labels(),
            classifier,
            new BatchQueue(testData.size()),
            config.concurrency(),
            terminationFlag
        );

        config.metrics().forEach(metric -> {
            double score = metric.compute(signedProbabilities, config.negativeClassWeight());
            trainingStatistics.addTestScore(metric, score);
        });
        progressTracker.endSubTask("Compute test metrics");
    }

    private List<TrainingExamplesSplit> trainValidationSplits(
        ReadOnlyHugeLongArray trainRelationshipIds,
        HugeLongArray actualLabels
    ) {
        var splitter = new StratifiedKFoldSplitter(
            pipeline.splitConfig().validationFolds(),
            trainRelationshipIds,
            actualLabels::get,
            config.randomSeed(),
            new TreeSet<>(classIdMap.originalIdsList())
        );
        return splitter.splits();
    }

    private void computeTrainMetric(
        FeaturesAndLabels trainData,
        Classifier classifier,
        ReadOnlyHugeLongArray evaluationSet,
        BiConsumer<Metric, Double> scoreConsumer
    ) {
        var signedProbabilities = SignedProbabilities.computeFromLabeledData(
            trainData.features(),
            trainData.labels(),
            classifier,
            new HugeBatchQueue(evaluationSet),
            config.concurrency(),
            terminationFlag
        );

        config.metrics().forEach(metric ->
            scoreConsumer.accept(
                metric,
                metric.compute(signedProbabilities, config.negativeClassWeight())
            )
        );
    }

    private Model<Classifier.ClassifierData, LinkPredictionTrainConfig, LinkPredictionModelInfo> createModel(
        TrainerConfig bestParameters,
        Classifier.ClassifierData classifierData,
        Map<Metric, BestMetricData> winnerMetrics
    ) {
        return Model.of(
            config.username(),
            config.modelName(),
            MODEL_TYPE,
            trainGraph.schema(),
            classifierData,
            config,
            LinkPredictionModelInfo.of(
                bestParameters,
                winnerMetrics,
                LinkPredictionPredictPipeline.from(pipeline)
            )
        );
    }

    @Override
    public void release() {

    }

    public static MemoryEstimation estimate(
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig trainConfig
    ) {
        // For the computation of the MemoryTree, this estimation assumes the given input graph dimensions to contain
        // the expected set sizes for the test and train relationshipTypes. That is, the graph dimensions input needs to
        // have the test and train relationship types as well as their relationship counts.

        var splitConfig = pipeline.splitConfig();

        var builder = MemoryEstimations.builder(LinkPredictionTrain.class);

        var fudgedLinkFeatureDim = MemoryRange.of(10, 500);

        int numberOfMetrics = trainConfig.metrics().size();
        return builder
            // After the training, the training features and labels are no longer accessed.
            // As the lifetimes of training and test data is non-overlapping, we assume the max is sufficient.
            .max("Features and labels", List.of(
                LinkFeaturesAndLabelsExtractor.estimate(
                    fudgedLinkFeatureDim,
                    relCounts -> relCounts.get(RelationshipType.of(splitConfig.trainRelationshipType())),
                    "Train"
                ),
                LinkFeaturesAndLabelsExtractor.estimate(
                    fudgedLinkFeatureDim,
                    relCounts -> relCounts.get(RelationshipType.of(splitConfig.testRelationshipType())),
                    "Test"
                )
            ))
            .add(estimateTrainingAndEvaluation(pipeline, fudgedLinkFeatureDim, numberOfMetrics))
            // we do not consider the training of the best model on the outer train set as the memory estimation is at most the maximum of the model training during the model selection
            // this assumes the training is independent of the relationship set size
            .add("Outer train stats map", StatsMap.memoryEstimation(numberOfMetrics, 1, 1))
            .add("Test stats map", StatsMap.memoryEstimation(numberOfMetrics, 1, 1))
            .fixed("Best model stats", numberOfMetrics * BestMetricData.estimateMemory())
            .build();
    }

    private static MemoryEstimation estimateTrainingAndEvaluation(
        LinkPredictionTrainingPipeline pipeline,
        MemoryRange linkFeatureDimension,
        int numberOfMetrics
    ) {
        var splitConfig = pipeline.splitConfig();
        var maxEstimationOverModelCandidates = maxEstimation(
            "Max over model candidates",
            pipeline.trainingParameterSpace()
                .values()
                .stream()
                .flatMap(List::stream)
                .flatMap(TunableTrainerConfig::streamCornerCaseConfigs)
                .map(trainerConfig -> MemoryEstimations.builder("Train and evaluate model")
                    .fixed("Stats map builder train", ModelStatsBuilder.sizeInBytes(numberOfMetrics))
                    .fixed("Stats map builder validation", ModelStatsBuilder.sizeInBytes(numberOfMetrics))
                    .max("Train model and compute train metrics", List.of(
                            estimateTraining(pipeline.splitConfig(), trainerConfig, linkFeatureDimension),
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
                    dim -> dim.relationshipCounts().get(RelationshipType.of(splitConfig.trainRelationshipType()))
                )
            )
            .add(maxEstimationOverModelCandidates)
            .add(
                "Inner train stats map",
                StatsMap.memoryEstimation(numberOfMetrics, pipeline.numberOfModelSelectionTrials(), 1)
            )
            .add(
                "Validation stats map",
                StatsMap.memoryEstimation(numberOfMetrics, pipeline.numberOfModelSelectionTrials(), 1)
            )
            .build();
    }

    private static MemoryEstimation estimateTraining(
        LinkPredictionSplitConfig splitConfig,
        TrainerConfig trainerConfig,
        MemoryRange linkFeatureDimension
    ) {
        return MemoryEstimations.setup(
            "Training", dim ->
                ClassifierTrainerFactory.memoryEstimation(
                    trainerConfig,
                    unused -> dim.relationshipCounts().get(RelationshipType.of(splitConfig.trainRelationshipType())),
                    2,
                    linkFeatureDimension,
                    true
                )
        );
    }

    private static MemoryEstimation estimateComputeTrainMetrics(LinkPredictionSplitConfig splitConfig) {
        return MemoryEstimations
            .builder("Compute train metrics")
            .perGraphDimension(
                "Sorted probabilities",
                (dim, threads) -> {
                    long trainSetSize = dim
                        .relationshipCounts()
                        .get(RelationshipType.of(splitConfig.trainRelationshipType()));
                    return MemoryRange.of(SignedProbabilities.estimateMemory(trainSetSize));
                }
            ).build();
    }
}
