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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.classification.F1Macro;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class CrossValidationTest {

    public static final double SCORE = 13.37;

    @Test
    void shouldLogProgress() {
        var log = new GdsTestLog();

        TestProgressTracker progressTracker = new TestProgressTracker(
            Tasks.task("test", CrossValidation.progressTasks(3, 2, 4)),
            log,
            new Concurrency(3),
            EmptyTaskRegistryFactory.INSTANCE
        );

        List<Metric> metrics = List.of(new F1Macro(LocalIdMap.of(0)));
        var trainingStatistics = new TrainingStatistics(metrics);

        var crossValidation = new CrossValidation<>(
            progressTracker,
            TerminationFlag.RUNNING_TRUE,
            metrics,
            3,
            Optional.empty(),
            (trainSet, modelParameters, metricsHandler, messageLogLevel) -> 0L,
            (evaluationSet, model, scoreConsumer) -> { scoreConsumer.consume(metrics.get(0), 0); }
        );

        progressTracker.beginSubTask("test");
        crossValidation.selectModel(
            ReadOnlyHugeLongArray.of(0, 1, 3, 7),
            (LongToLongFunction) longParameter -> 0,
            new TreeSet<>(List.of(0L)),
            trainingStatistics,
            List.<TrainerConfig>of(new TestTrainerConfig("a"), new TestTrainerConfig("b")).iterator()
        );
        progressTracker.endSubTask("test");

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "test :: Start",
                "test :: Create validation folds :: Start",
                "test :: Create validation folds 100%",
                "test :: Create validation folds :: Finished",
                "test :: Select best model :: Start",
                "test :: Select best model :: Trial 1 of 2 :: Start",
                "test :: Select best model :: Trial 1 of 2 :: Method: RandomForest, Parameters: {name=a}",
                "test :: Select best model :: Trial 1 of 2 33%",
                "test :: Select best model :: Trial 1 of 2 66%",
                "test :: Select best model :: Trial 1 of 2 100%",
                "test :: Select best model :: Trial 1 of 2 :: Main validation metric (F1_MACRO): 0.0000",
                "test :: Select best model :: Trial 1 of 2 :: Validation metrics: {F1_MACRO=0.0}",
                "test :: Select best model :: Trial 1 of 2 :: Training metrics: {F1_MACRO=0.0}",
                "test :: Select best model :: Trial 1 of 2 :: Finished",
                "test :: Select best model :: Trial 2 of 2 :: Start",
                "test :: Select best model :: Trial 2 of 2 :: Method: RandomForest, Parameters: {name=b}",
                "test :: Select best model :: Trial 2 of 2 33%",
                "test :: Select best model :: Trial 2 of 2 66%",
                "test :: Select best model :: Trial 2 of 2 100%",
                "test :: Select best model :: Trial 2 of 2 :: Main validation metric (F1_MACRO): 0.0000",
                "test :: Select best model :: Trial 2 of 2 :: Validation metrics: {F1_MACRO=0.0}",
                "test :: Select best model :: Trial 2 of 2 :: Training metrics: {F1_MACRO=0.0}",
                "test :: Select best model :: Trial 2 of 2 :: Finished",
                "test :: Select best model :: Best trial was Trial 1 with main validation metric 0.0000",
                "test :: Select best model :: Finished",
                "test :: Finished"
            );
    }

}
