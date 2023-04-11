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
package org.neo4j.gds.ml.training;

import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.LogLevel;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.MetricConsumer;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.ModelSpecificMetricsHandler;
import org.neo4j.gds.ml.metrics.ModelStatsBuilder;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.splitting.StratifiedKFoldSplitter;
import org.neo4j.gds.ml.splitting.TrainingExamplesSplit;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CrossValidation<MODEL_TYPE> {

    private final ProgressTracker progressTracker;

    private final TerminationFlag terminationFlag;
    private final List<? extends Metric> metrics;
    private final int validationFolds;
    private final Optional<Long> randomSeed;
    private final ModelTrainer<MODEL_TYPE> modelTrainer;
    private final ModelEvaluator<MODEL_TYPE> modelEvaluator;

    public static List<Task> progressTasks(int validationFolds, int numberOfModelSelectionTrials, long trainSetSize) {
        return List.of(
            Tasks.leaf("Create validation folds", Math.max((long) (0.5 * trainSetSize), 1)),
            Tasks.iterativeFixed(
                "Select best model",
                () -> List.of(Tasks.leaf("Trial", 5L * validationFolds * trainSetSize)),
                numberOfModelSelectionTrials
            )
        );
    }

    public CrossValidation(
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        List<? extends Metric> metrics,
        int validationFolds,
        Optional<Long> randomSeed,
        ModelTrainer<MODEL_TYPE> modelTrainer,
        ModelEvaluator<MODEL_TYPE> modelEvaluator
    ) {
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.metrics = metrics;
        this.validationFolds = validationFolds;
        this.randomSeed = randomSeed;
        this.modelTrainer = modelTrainer;
        this.modelEvaluator = modelEvaluator;
    }

    public void selectModel(
        ReadOnlyHugeLongArray outerTrainSet,
        LongToLongFunction targets,
        SortedSet<Long> distinctInternalTargets,
        TrainingStatistics trainingStatistics,
        Iterator<TrainerConfig> modelCandidates
    ) {
        progressTracker.beginSubTask("Create validation folds");
        List<TrainingExamplesSplit> validationSplits = new StratifiedKFoldSplitter(
            validationFolds,
            outerTrainSet,
            targets,
            randomSeed,
            distinctInternalTargets
        ).splits();
        progressTracker.endSubTask("Create validation folds");

        progressTracker.beginSubTask("Select best model");
        int trial = 0;
        while (modelCandidates.hasNext()) {
            progressTracker.beginSubTask("Trial");
            progressTracker.setSteps(validationSplits.size());

            terminationFlag.assertRunning();

            var modelParams = modelCandidates.next();
            progressTracker.logInfo(formatWithLocale(
                "Method: %s, Parameters: %s",
                modelParams.method(),
                modelParams.toMap()
            ));

            var validationStatsBuilder = new ModelStatsBuilder(validationSplits.size());
            var trainStatsBuilder = new ModelStatsBuilder(validationSplits.size());
            var metricsHandler = ModelSpecificMetricsHandler.of(metrics, validationStatsBuilder);

            int fold = 1;
            for (TrainingExamplesSplit split : validationSplits) {
                var trainSet = split.trainSet();
                var validationSet = split.testSet();

                progressTracker.logDebug("Starting fold " + fold + " training");
                var trainedModel = modelTrainer.train(trainSet, modelParams, metricsHandler, LogLevel.DEBUG);
                progressTracker.logDebug("Finished fold " + fold + " training");

                modelEvaluator.evaluate(validationSet, trainedModel, validationStatsBuilder::update);
                modelEvaluator.evaluate(trainSet, trainedModel, trainStatsBuilder::update);

                progressTracker.logSteps(1);

                fold++;
            }

            var candidateStats = ModelCandidateStats.of(
                modelParams,
                trainStatsBuilder.build(),
                validationStatsBuilder.build()
            );
            trainingStatistics.addCandidateStats(candidateStats);

            var validationStats = trainingStatistics.validationMetricsAvg(trial);
            var trainStats = trainingStatistics.trainMetricsAvg(trial);
            double mainMetric = trainingStatistics.getMainMetric(trial);

            progressTracker.logInfo(formatWithLocale(
                "Main validation metric (%s): %.4f",
                trainingStatistics.evaluationMetric(),
                mainMetric
            ));
            progressTracker.logInfo(formatWithLocale("Validation metrics: %s", validationStats));
            progressTracker.logInfo(formatWithLocale("Training metrics: %s", trainStats));

            trial++;

            progressTracker.endSubTask("Trial");
        }

        int bestTrial = trainingStatistics.getBestTrialIdx() + 1;
        double bestTrialScore = trainingStatistics.getBestTrialScore();
        progressTracker.logInfo(formatWithLocale(
            "Best trial was Trial %d with main validation metric %.4f",
            bestTrial,
            bestTrialScore
        ));

        progressTracker.endSubTask("Select best model");
    }

    @FunctionalInterface
    public interface ModelTrainer<MODEL_TYPE> {
        MODEL_TYPE train(
            ReadOnlyHugeLongArray trainSet,
            TrainerConfig modelParameters,
            ModelSpecificMetricsHandler metricsHandler,
            LogLevel messageLogLevel
        );
    }

    @FunctionalInterface
    public interface ModelEvaluator<MODEL_TYPE> {
        void evaluate(ReadOnlyHugeLongArray evaluationSet, MODEL_TYPE model, MetricConsumer scoreConsumer);
    }
}
