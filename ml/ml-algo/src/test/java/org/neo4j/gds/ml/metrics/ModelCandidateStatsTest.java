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
package org.neo4j.gds.ml.metrics;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;

import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.ml.metrics.LinkMetric.AUCPR;
import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.ml.metrics.regression.RegressionMetrics.ROOT_MEAN_SQUARED_ERROR;

class ModelCandidateStatsTest {
    @Test
    void testRender() {
        var candidateStats = ModelCandidateStats.of(
            LogisticRegressionTrainConfig.DEFAULT,
            Map.of(OUT_OF_BAG_ERROR, EvaluationScores.of(
                0.33,
                0.13,
                0.13
            )),
            Map.of(
                ROOT_MEAN_SQUARED_ERROR, EvaluationScores.of(
                    0.3,
                    0.1,
                    0.1
                ),
                AUCPR, EvaluationScores.of(
                    0.4,
                    0.2,
                    0.2
                )
            )
        );
        var renderedStats = candidateStats.renderMetrics(
            Map.of(ROOT_MEAN_SQUARED_ERROR, 0.22, AUCPR, 0.33),
            Map.of(ROOT_MEAN_SQUARED_ERROR, 0.55, AUCPR, 0.66)
        );

        var expected = Map.of(
            "OUT_OF_BAG_ERROR", Map.of(
                "train", Map.of("avg", 0.33, "max", 0.13, "min", 0.13)
            ),
            "AUCPR", Map.of(
                "outerTrain", 0.66,
                "test", 0.33,
                "validation", Map.of("avg", 0.4, "max", 0.2, "min", 0.2)
            ),
            "ROOT_MEAN_SQUARED_ERROR", Map.of(
                "outerTrain", 0.55,
                "test", 0.22,
                "validation", Map.of("avg", 0.3, "max", 0.1, "min", 0.1)
            )
        );
        assertThat(renderedStats).isEqualTo(expected);
    }
}
