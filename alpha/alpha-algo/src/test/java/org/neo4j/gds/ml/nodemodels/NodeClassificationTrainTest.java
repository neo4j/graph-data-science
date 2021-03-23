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
package org.neo4j.gds.ml.nodemodels;

import org.assertj.core.data.Percentage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeClassificationTrainTest {

    // TODO validation
    // at least one config

    @GdlGraph
    private static final String DB_QUERY =
        "  (:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})";

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @EnumSource(Metric.class)
    void selectsTheBestModel(Metric metric) {

        Map<String, Object> model1 = Map.of("penalty", 1, "maxIterations", 0);
        Map<String, Object> model2 = Map.of("penalty", 1, "maxIterations", 10000, "tolerance", 1e-5);

        Map<String, Object> expectedWinner = model2;
        var log = new TestLog();
        var config = createConfig(
            List.of(model1, model2),
            "model",
            List.of("a", "b"),
            metric,
            1L
        );

        var ncTrain = new NodeClassificationTrain(
            graph,
            config,
            log
        );

        var model = ncTrain.compute();

        var customInfo = (NodeClassificationModelInfo) model.customInfo();
        var validationScores = customInfo.metrics().get(metric).validation();

        assertThat(validationScores).hasSize(2);
        var actualWinnerParams = customInfo.bestParameters();
        assertThat(actualWinnerParams).containsAllEntriesOf(expectedWinner);
        double model1Score = validationScores.get(0).avg();
        double model2Score = validationScores.get(1).avg();
        assertThat(model1Score).isNotCloseTo(model2Score, Percentage.withPercentage(0.2));
    }

    @GdsEditionTest(Edition.EE)
    @ParameterizedTest
    @EnumSource(Metric.class)
    void shouldProduceDifferentMetricsForDifferentTrainings(Metric metric) {

        var log = new TestLog();
        var modelCandidates = List.of(
            Map.<String, Object>of("penalty", 0.0625, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 0.125, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 0.25, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 0.5, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 1.0, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 2.0, "maxIterations", 1000),
            Map.<String, Object>of("penalty", 4.0, "maxIterations", 1000)
        );

        var bananasConfig = createConfig(
            modelCandidates,
            "bananasModel",
            List.of("bananas"),
            metric,
            1337L
        );
        var bananasTrain = new NodeClassificationTrain(
            graph,
            bananasConfig,
            log
        );

        var arrayPropertyConfig = createConfig(
            modelCandidates,
            "arrayPropertyModel",
            List.of("arrayProperty"),
            metric,
            42L
        );
        var arrayPropertyTrain = new NodeClassificationTrain(
            graph,
            arrayPropertyConfig,
            log
        );

        var bananasModel = bananasTrain.compute();
        var arrayPropertyModel = arrayPropertyTrain.compute();

        assertThat(arrayPropertyModel)
            .usingRecursiveComparison()
            .withFailMessage("The trained models are exactly the same instance!")
            .isNotSameAs(bananasModel);

        assertThat(arrayPropertyModel.data())
            .usingRecursiveComparison()
            .withFailMessage("Should not produce the same trained `data`!")
            .isNotEqualTo(bananasModel.data());

        var bananasCustomInfo = (NodeClassificationModelInfo) bananasModel.customInfo();
        var bananasValidationScore = bananasCustomInfo.metrics().get(metric);

        var arrayPropertyCustomInfo = (NodeClassificationModelInfo) arrayPropertyModel.customInfo();
        var arrayPropertyValidationScores = arrayPropertyCustomInfo.metrics().get(metric);

        assertThat(arrayPropertyValidationScores)
            .usingRecursiveComparison()
            .isNotSameAs(bananasValidationScore)
            .isNotEqualTo(bananasValidationScore);
    }

    private NodeClassificationTrainConfig createConfig(
        Iterable<Map<String, Object>> modelCandidates,
        String modelName,
        Iterable<String> featureProperties,
        Metric metric, long randomSeed
    ) {
        return ImmutableNodeClassificationTrainConfig.builder()
            .modelName(modelName)
            .featureProperties(featureProperties)
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(1)
            .randomSeed(randomSeed)
            .targetProperty("t")
            .metrics(List.of(metric))
            .params(modelCandidates)
            .build();
    }
}
