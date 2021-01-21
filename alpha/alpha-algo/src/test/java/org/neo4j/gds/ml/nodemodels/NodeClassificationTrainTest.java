/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.nodemodels.logisticregression.ImmutableNodeClassificationTrainConfig;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.nodemodels.metrics.Metric.F1_MACRO;

@GdlExtension
class NodeClassificationTrainTest {

    // TODO validation
    // at least one config

    @GdlGraph
    private static final String DB_QUERY =
        "  (:N {a: 1.2, b: 1.2, t: 0})" +
        ", (:N {a: 2.8, b: 2.5, t: 0})" +
        ", (:N {a: 3.3, b: 0.5, t: 0})" +
        ", (:N {a: 1.0, b: 0.5, t: 0})" +
        ", (:N {a: 1.32, b: 0.5, t: 0})" +
        ", (:N {a: 1.3, b: 1.5, t: 1})" +
        ", (:N {a: 5.3, b: 10.5, t: 1})" +
        ", (:N {a: 1.3, b: 2.5, t: 1})" +
        ", (:N {a: 0.0, b: 66.8, t: 1})" +
        ", (:N {a: 0.1, b: 2.8, t: 1})" +
        ", (:N {a: 0.66, b: 2.8, t: 1})" +
        ", (:N {a: 2.0, b: 10.8, t: 1})" +
        ", (:N {a: 5.0, b: 7.8, t: 1})" +
        ", (:N {a: 4.0, b: 5.8, t: 1})" +
        ", (:N {a: 1.0, b: 0.9, t: 1})";

    @Inject
    TestGraph graph;

    @Test
    void selectsTheBestModel() {

        Map<String, Object> model1 = Map.of( "penalty", 1, "maxIterations", 0);
        Map<String, Object> model2 = Map.of( "penalty", 1, "maxIterations", 10000, "tolerance", 1e-5);

        Map<String, Object> expectedWinner = model2;
        var log = new TestLog();
        var ncTrain = new NodeClassificationTrain(
            graph,
            ImmutableNodeClassificationTrainConfig.builder()
                .modelName("model")
                .featureProperties(List.of("a", "b"))
                .holdoutFraction(0.33)
                .validationFolds(2)
                .concurrency(1)
                .randomSeed(1L)
                .targetProperty("t")
                .metrics(List.of(F1_MACRO))
                .params(List.of(model1, model2))
                .build(),
            log
        );

        var model = ncTrain.compute();

        var validationScores = model.customInfo().metrics().get(F1_MACRO).validation();

        assertThat(validationScores).hasSize(2);
        var actualWinnerParams = model.customInfo().bestParameters();
        assertThat(actualWinnerParams).containsAllEntriesOf(expectedWinner);
        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        assertThat(model1Score).isNotCloseTo(model2Score, Percentage.withPercentage(0.2));
    }
}
