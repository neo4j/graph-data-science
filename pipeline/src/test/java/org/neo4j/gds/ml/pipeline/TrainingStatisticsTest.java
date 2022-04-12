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
package org.neo4j.gds.ml.pipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.metrics.classification.AllClassMetric;
import org.neo4j.gds.ml.metrics.BestMetricData;
import org.neo4j.gds.ml.metrics.BestModelStats;
import org.neo4j.gds.ml.metrics.ImmutableModelStats;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.metrics.classification.AllClassMetric.F1_WEIGHTED;
import static org.neo4j.gds.ml.metrics.LinkMetric.AUCPR;

class TrainingStatisticsTest {

    @Test
    void selectsBestParametersAccordingToMainMetric() {
        var trainingStatistics = new TrainingStatistics(List.of(AUCPR, F1_WEIGHTED));

        trainingStatistics.addValidationStats(AUCPR, ModelStats.of(
            new TestTrainerConfig("bad"),
            0.1,
            1000,
            1000
        ));
        trainingStatistics.addValidationStats(AUCPR, ModelStats.of(
            new TestTrainerConfig("better"),
            0.2,
            0.2,
            0.2
        ));
        trainingStatistics.addValidationStats(F1_WEIGHTED, ModelStats.of(
            new TestTrainerConfig("notprimarymetric"),
            5000,
            5000,
            5000
        ));

        assertThat(trainingStatistics.bestParameters().methodName()).isEqualTo("better");
    }

    @Test
    void getsMetricsForWinningModel() {
        var trainingStatistics = new TrainingStatistics(List.of(AUCPR, F1_WEIGHTED));

        var candidate = new TestTrainerConfig("train");
        ModelStats trainStats = ModelStats.of(
            candidate,
            0.1,
            0.1,
            0.1
        );
        ModelStats validationStats = ModelStats.of(
            candidate,
            0.4,
            0.3,
            0.5
        );
        trainingStatistics.addTrainStats(AUCPR, trainStats);
        trainingStatistics.addTrainStats(F1_WEIGHTED, trainStats);
        trainingStatistics.addValidationStats(AUCPR, validationStats);
        trainingStatistics.addValidationStats(F1_WEIGHTED, validationStats);
        trainingStatistics.addTestScore(AUCPR, 1);
        trainingStatistics.addTestScore(F1_WEIGHTED, 2);
        trainingStatistics.addOuterTrainScore(AUCPR, 3);
        trainingStatistics.addOuterTrainScore(F1_WEIGHTED, 4);

        var winningModelMetrics = trainingStatistics.metricsForWinningModel();

        assertThat(winningModelMetrics)
            .hasSize(2)
            .containsEntry(
                AUCPR,
                BestMetricData.of(BestModelStats.of(trainStats), BestModelStats.of(validationStats), 3, 1)
            )
            .containsEntry(
                F1_WEIGHTED,
                BestMetricData.of(BestModelStats.of(trainStats), BestModelStats.of(validationStats), 4, 2)
            );

    }

    @Test
    void toMap() {
        RandomForestTrainConfig firstCandidate = RandomForestTrainConfig.DEFAULT;
        LogisticRegressionTrainConfig secondCandidate = LogisticRegressionTrainConfig.DEFAULT;

        var selectResult = new TrainingStatistics(List.of(AllClassMetric.ACCURACY));

        selectResult.addTrainStats(
            AllClassMetric.ACCURACY,
            ImmutableModelStats.of(firstCandidate, 0.33, 0.1, 0.6)
        );
        selectResult.addTrainStats(
            AllClassMetric.ACCURACY,
            ImmutableModelStats.of(secondCandidate, 0.2, 0.01, 0.7)
        );

        selectResult.addValidationStats(
            AllClassMetric.ACCURACY,
            ImmutableModelStats.of(firstCandidate, 0.4, 0.3, 0.5)
        );
        selectResult.addValidationStats(
            AllClassMetric.ACCURACY,
            ImmutableModelStats.of(secondCandidate, 0.8, 0.7, 0.9)
        );


        List<Map<String, Object>> expectedTrainAccuracyStats = List.of(
            Map.of("params", firstCandidate.toMap(), "avg", 0.33, "min", 0.1, "max", 0.6),
            Map.of("params", secondCandidate.toMap(), "avg", 0.2, "min", 0.01, "max", 0.7)
        );

        List<Map<String, Object>> expectedValidationAccuracyStats = List.of(
            Map.of("params", firstCandidate.toMap(), "avg", 0.4, "min", 0.3, "max", 0.5),
            Map.of("params", secondCandidate.toMap(), "avg", 0.8, "min", 0.7, "max", 0.9)
        );

        assertThat(selectResult.toMap())
            .containsEntry("bestParameters", secondCandidate.toMap())
            .containsEntry("trainStats", Map.of("ACCURACY", expectedTrainAccuracyStats))
            .containsEntry("validationStats", Map.of("ACCURACY", expectedValidationAccuracyStats));
    }

    private static final class TestTrainerConfig implements TrainerConfig {

        private final String method;

        private TestTrainerConfig(String method) {this.method = method;}

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }

        @Override
        public String methodName() {
            return method;
        }
    }

}
