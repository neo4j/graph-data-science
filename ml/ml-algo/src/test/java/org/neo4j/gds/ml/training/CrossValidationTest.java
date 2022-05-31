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
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.metrics.classification.AllClassMetric.F1_MACRO;

class CrossValidationTest {

    public static final double SCORE = 13.37;

    @Test
    void shouldLogProgress() {
        TestLog log = Neo4jProxy.testLog();

        TestProgressTracker progressTracker = new TestProgressTracker(
            CrossValidation.progressTask(3, 2, 4),
            log,
            3,
            EmptyTaskRegistryFactory.INSTANCE
        );

        List<Metric> metrics = List.of(F1_MACRO);
        var trainingStatistics = new TrainingStatistics(metrics);

        var crossValidation = new CrossValidation<>(
            progressTracker,
            TerminationFlag.RUNNING_TRUE,
            metrics,
            3,
            Optional.empty(),
            (trainSet, modelParameters, metricsHandler) -> 0L,
            (evaluationSet, model, scoreConsumer) -> { scoreConsumer.consume(F1_MACRO, 0); }
        );

        crossValidation.selectModel(
            ReadOnlyHugeLongArray.of(0, 1, 3, 7),
            (LongToLongFunction) longParameter -> 0,
            new TreeSet<>(List.of(0L)),
            trainingStatistics,
            List.<TrainerConfig>of(new TestTrainerConfig("a"), new TestTrainerConfig("b")).iterator()
        );

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "Select best model :: Start",
                "Select best model :: Trial 1 of 2 :: Start",
                "Select best model :: Trial 1 of 2 :: Method: RandomForest, Parameters: {name=a}",
                "Select best model :: Trial 1 of 2 33%",
                "Select best model :: Trial 1 of 2 66%",
                "Select best model :: Trial 1 of 2 100%",
                "Select best model :: Trial 1 of 2 :: Main validation metric (F1_MACRO): 0.0000",
                "Select best model :: Trial 1 of 2 :: Validation metrics: {F1_MACRO=0.0}",
                "Select best model :: Trial 1 of 2 :: Training metrics: {F1_MACRO=0.0}",
                "Select best model :: Trial 1 of 2 :: Finished",
                "Select best model :: Trial 2 of 2 :: Start",
                "Select best model :: Trial 2 of 2 :: Method: RandomForest, Parameters: {name=b}",
                "Select best model :: Trial 2 of 2 33%",
                "Select best model :: Trial 2 of 2 66%",
                "Select best model :: Trial 2 of 2 100%",
                "Select best model :: Trial 2 of 2 :: Main validation metric (F1_MACRO): 0.0000",
                "Select best model :: Trial 2 of 2 :: Validation metrics: {F1_MACRO=0.0}",
                "Select best model :: Trial 2 of 2 :: Training metrics: {F1_MACRO=0.0}",
                "Select best model :: Trial 2 of 2 :: Finished",
                "Select best model :: Best trial was Trial 1 with main validation metric 0.0000",
                "Select best model :: Finished"
            );
    }

}
