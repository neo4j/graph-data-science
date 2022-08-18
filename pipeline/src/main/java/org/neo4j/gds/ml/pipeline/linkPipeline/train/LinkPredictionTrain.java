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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.ReadOnlyHugeLongIdentityArray;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.ImmutableModelStats;
import org.neo4j.gds.ml.metrics.MetricConsumer;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.metrics.SignedProbabilities;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.ClassifierTrainer;
import org.neo4j.gds.ml.models.ClassifierTrainerFactory;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.automl.RandomSearch;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.splitting.EdgeSplitter;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;
import org.neo4j.gds.ml.training.CrossValidation;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.utils.mem.MemoryEstimations.maxEstimation;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkFeaturesAndLabelsExtractor.extractFeaturesAndLabels;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LinkPredictionTrain {

    private final Graph trainGraph;
    private final Graph validationGraph;
    private final LinkPredictionTrainingPipeline pipeline;
    private final LinkPredictionTrainConfig config;
    private final LocalIdMap classIdMap;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;


    public static LocalIdMap makeClassIdMap() {
        return LocalIdMap.of((long) EdgeSplitter.NEGATIVE, (long) EdgeSplitter.POSITIVE);
    }

    public LinkPredictionTrain(
        Graph trainGraph,
        Graph validationGraph,
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig config,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.trainGraph = trainGraph;
        this.validationGraph = validationGraph;
        this.pipeline = pipeline;
        this.config = config;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
        this.classIdMap = makeClassIdMap();
    }

    public static List<Task> progressTasks(
        long relationshipCount,
        LinkPredictionSplitConfig splitConfig,
        int numberOfModelSelectionTrials
    ) {
        var sizes = splitConfig.expectedSetSizes(relationshipCount);

        var tasks = new ArrayList<Task>();
        tasks.add(Tasks.leaf("Extract train features", sizes.trainSize() * 3));
        tasks.addAll(CrossValidation.progressTasks(
            splitConfig.validationFolds(),
            numberOfModelSelectionTrials,
            sizes.trainSize()
        ));
        tasks.add(ClassifierTrainer.progressTask("Train best model", sizes.trainSize() * 5));
        tasks.add(Tasks.leaf("Compute train metrics", sizes.trainSize()));
        tasks.add(Tasks.task(
                "Evaluate on test data",
                Tasks.leaf("Extract test features", sizes.testSize() * 3),
                Tasks.leaf("Compute test metrics", sizes.testSize())
            )
        );

        return tasks;
    }

    @Deprecated
    public static long estimateMemory() {
        return MemoryUsage.sizeOfInstance(ImmutableModelStats.class) * 2 + Double.BYTES * 2;
    }

    public LinkPredictionTrainResult compute() {
        progressTracker.beginSubTask("Extract train features");
        var trainData = extractFeaturesAndLabels(
            trainGraph,
            pipeline.featureSteps(),
            config.concurrency(),
            progressTracker,
            terminationFlag
        );
        var trainRelationshipIds = new ReadOnlyHugeLongIdentityArray(trainData.size());
        progressTracker.endSubTask("Extract train features");

        var trainingStatistics = new TrainingStatistics(config.metrics());

        findBestModelCandidate(trainData, trainRelationshipIds, trainingStatistics);

        // train best model on the entire training graph
        progressTracker.beginSubTask("Train best model");
        var classifier = trainModel(
            trainData,
            trainRelationshipIds,
            trainingStatistics.bestParameters(),
            LogLevel.INFO,
            ModelSpecificMetricsHandler.of(config.metrics(), trainingStatistics::addTestScore)
        );
        progressTracker.endSubTask("Train best model");

        // evaluate the best model on the training and test graphs
        progressTracker.beginSubTask("Compute train metrics");
        computeTrainMetric(
            trainData,
            classifier,
            trainRelationshipIds,
            trainingStatistics::addOuterTrainScore,
            progressTracker
        );
        progressTracker.endSubTask("Compute train metrics");

        var outerTrainMetrics = trainingStatistics.winningModelOuterTrainMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on full train set: %s", outerTrainMetrics));

        progressTracker.beginSubTask("Evaluate on test data");
        computeTestMetric(classifier, trainingStatistics);
        progressTracker.endSubTask("Evaluate on test data");

        var testMetrics = trainingStatistics.winningModelTestMetrics();
        progressTracker.logInfo(formatWithLocale("Final model metrics on test set: %s", testMetrics));

        return ImmutableLinkPredictionTrainResult.of(classifier, trainingStatistics);
    }

    private void findBestModelCandidate(
        FeaturesAndLabels trainData,
        ReadOnlyHugeLongArray trainRelationshipIds,
        TrainingStatistics trainingStatistics
    ) {
        var modelCandidates = new RandomSearch(
            pipeline.trainingParameterSpace(),
            pipeline.autoTuningConfig().maxTrials(),
            config.randomSeed()
        );

        var crossValidation = new CrossValidation<>(
            progressTracker,
            terminationFlag,
            config.metrics(),
            pipeline.splitConfig().validationFolds(),
            config.randomSeed(),
            (trainSet, modelParameters, metricsHandler, messageLogLevel) -> trainModel(
                trainData,
                trainSet,
                modelParameters,
                messageLogLevel,
                metricsHandler
            ),
            (evaluationSet, classifier, scoreConsumer) -> computeTrainMetric(
                trainData,
                classifier,
                evaluationSet,
                scoreConsumer,
                ProgressTracker.NULL_TRACKER
            )
        );

        crossValidation.selectModel(
            trainRelationshipIds,
            trainData.labels()::get,
            new TreeSet<>(classIdMap.originalIdsList()),
            trainingStatistics,
            modelCandidates
        );
    }

    @NotNull
    private Classifier trainModel(
        FeaturesAndLabels featureAndLabels,
        ReadOnlyHugeLongArray trainSet,
        TrainerConfig trainerConfig,
        LogLevel messageLogLevel,
        ModelSpecificMetricsHandler metricsHandler
    ) {
        return ClassifierTrainerFactory.create(
            trainerConfig,
            classIdMap,
            terminationFlag,
            progressTracker,
            messageLogLevel,
            config.concurrency(),
            config.randomSeed(),
            true,
            metricsHandler
        ).train(featureAndLabels.features(), featureAndLabels.labels(), trainSet);
    }

    private void computeTestMetric(Classifier classifier, TrainingStatistics trainingStatistics) {
        progressTracker.beginSubTask("Extract test features");
        var testData = extractFeaturesAndLabels(
            validationGraph,
            pipeline.featureSteps(),
            config.concurrency(),
            progressTracker,
            terminationFlag
        );
        progressTracker.endSubTask("Extract test features");

        progressTracker.beginSubTask("Compute test metrics");
        var signedProbabilities = SignedProbabilities.computeFromLabeledData(
            testData.features(),
            testData.labels(),
            classifier,
            BatchQueue.consecutive(testData.size()),
            config.concurrency(),
            terminationFlag,
            progressTracker
        );

        config.linkMetrics()
            .forEach(metric -> {
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
        MetricConsumer metricConsumer,
        ProgressTracker progressTracker
    ) {
        var signedProbabilities = SignedProbabilities.computeFromLabeledData(
            trainData.features(),
            trainData.labels(),
            classifier,
            BatchQueue.fromArray(evaluationSet),
            config.concurrency(),
            terminationFlag,
            progressTracker
        );

        config.linkMetrics().forEach(metric ->
            metricConsumer.consume(
                metric,
                metric.compute(signedProbabilities, config.negativeClassWeight())
            )
        );
    }

    public static MemoryEstimation estimate(
        LinkPredictionTrainingPipeline pipeline,
        LinkPredictionTrainConfig trainConfig
    ) {
        // For the computation of the MemoryTree, this estimation assumes the given input graph dimensions to contain
        // the expected set sizes for the test and train relationshipTypes. That is, the graph dimensions input needs to
        // have the test and train relationship types as well as their relationship counts.

        var splitConfig = pipeline.splitConfig();

        var builder = MemoryEstimations.builder(LinkPredictionTrain.class.getSimpleName());

        var fudgedLinkFeatureDim = MemoryRange.of(10, 500);

        int numberOfMetrics = trainConfig.linkMetrics().size();
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
            .add("Outer train stats map", TrainingStatistics.memoryEstimationStatsMap(numberOfMetrics, 1, 1))
            .add("Test stats map", TrainingStatistics.memoryEstimationStatsMap(numberOfMetrics, 1, 1))
            .fixed("Best model stats", numberOfMetrics * estimateMemory())
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
                TrainingStatistics.memoryEstimationStatsMap(numberOfMetrics, pipeline.numberOfModelSelectionTrials(), 1)
            )
            .add(
                "Validation stats map",
                TrainingStatistics.memoryEstimationStatsMap(numberOfMetrics, pipeline.numberOfModelSelectionTrials(), 1)
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
