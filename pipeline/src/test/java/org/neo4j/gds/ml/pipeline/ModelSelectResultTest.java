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
import org.neo4j.gds.ml.metrics.AllClassMetric;
import org.neo4j.gds.ml.metrics.ImmutableModelStats;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModelSelectResultTest {

    @Test
    void toMap() {
        RandomForestTrainConfig firstCandidate = RandomForestTrainConfig.DEFAULT;
        LogisticRegressionTrainConfig secondCandidate = LogisticRegressionTrainConfig.DEFAULT;

        var trainStats = Map.of(
            AllClassMetric.ACCURACY,
            List.of(
                ImmutableModelStats.of(firstCandidate, 0.33, 0.1, 0.6),
                ImmutableModelStats.of(secondCandidate, 0.2, 0.01, 0.7)
            )
        );

        var validationStats = Map.of(
            AllClassMetric.ACCURACY,
            List.of(
                ImmutableModelStats.of(firstCandidate, 0.4, 0.3, 0.5),
                ImmutableModelStats.of(secondCandidate, 0.8, 0.7, 0.9)
            )
        );


        var selectResult = ModelSelectResult.of(firstCandidate, trainStats, validationStats);

        List<Map<String, Object>> expectedTrainAccuracyStats = List.of(
            Map.of("params", firstCandidate.toMap(), "avg", 0.33, "min", 0.1, "max", 0.6),
            Map.of("params", secondCandidate.toMap(), "avg", 0.2, "min", 0.01, "max", 0.7)
        );

        List<Map<String, Object>> expectedValidationAccuracyStats = List.of(
            Map.of("params", firstCandidate.toMap(), "avg", 0.4, "min", 0.3, "max", 0.5),
            Map.of("params", secondCandidate.toMap(), "avg", 0.8, "min", 0.7, "max", 0.9)
        );

        assertThat(selectResult.toMap())
            .containsEntry("bestParameters", firstCandidate.toMap())
            .containsEntry("trainStats", Map.of("ACCURACY", expectedTrainAccuracyStats))
            .containsEntry("validationStats", Map.of("ACCURACY", expectedValidationAccuracyStats))
        ;
    }

}
