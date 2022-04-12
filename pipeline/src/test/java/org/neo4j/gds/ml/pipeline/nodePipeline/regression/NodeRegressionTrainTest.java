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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfigImpl;
import org.neo4j.gds.ml.models.linearregression.LinearRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestTrainerConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeRegressionTrainTest {

    @GdlGraph
    private static final String DB_QUERY =
        "({scalar: 1.5,     target:   3 })," +
        "({scalar: 2.5,     target:   5 })," +
        "({scalar: 3.5,     target:   7 })," +
        "({scalar: 4.5,     target:   9 })," +
        "({scalar: 5.5,     target:  11 })," +
        "({scalar: 6.5,     target:  13 })," +
        "({scalar: 7.5,     target:  15 })," +
        "({scalar: 8.5,     target:  17 })," +
        "({scalar: 9.5,     target:  19 })," +
        "({scalar: 10.5,    target:  21 })," +
        "({scalar: -5.5,    target: -11 })," +
        "({scalar: -12.5,   target: -25 })," +
        "({scalar: 42.5,    target:  85 }),";

    @Inject
    TestGraph graph;

    @Test
    void trainWithOnlyLR() {
        LinearRegressionTrainConfig candidate1 = LinearRegressionTrainConfig.DEFAULT;
        LinearRegressionTrainConfig candidate2 = LinearRegressionTrainConfigImpl.builder().maxEpochs(5).build();

        var modelCandidates = Map.of(
            TrainingMethod.LinearRegression, Stream.of(candidate1, candidate2)
                .map(config -> TunableTrainerConfig.of(config.toMap(), TrainingMethod.valueOf(config.methodName())))
                .collect(Collectors.toList())
        );

        NodeRegressionPipelineTrainConfig trainConfig = NodeRegressionPipelineTrainConfigImpl.builder()
            .username("DUMMY")
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .targetProperty("target")
            .randomSeed(42L)
            .metrics(List.of(RegressionMetrics.MEAN_SQUARED_ERROR.name()))
            .build();

        var nrTrain = NodeRegressionTrain.create(
            graph,
            List.of("scalar"),
            NodePropertyPredictionSplitConfig.DEFAULT_CONFIG,
            modelCandidates,
            2,
            trainConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        NodeRegressionTrainResult result = nrTrain.compute();
        var trainingStatistics = result.trainingStatistics();

        assertThat(result.regressor()).isInstanceOf(LinearRegressor.class);
        assertThat(trainingStatistics.bestParameters().toMap()).isEqualTo(candidate1.toMap());

        var bestMetricData = trainingStatistics.metricsForWinningModel().get(RegressionMetrics.MEAN_SQUARED_ERROR);

        // FIXME change these with actual loss implementation
        assertThat(bestMetricData.outerTrain()).isEqualTo(259.52916, Offset.offset(1e-5));
        assertThat(bestMetricData.test()).isEqualTo(2109.82749,  Offset.offset(1e-5));
    }

    @Test
    void trainWithOnlyRF() {
        var candidate1 = RandomForestTrainerConfig.DEFAULT;
        var candidate2 = RandomForestTrainerConfigImpl.builder().numberOfDecisionTrees(20).build();

        var modelCandidates = Map.of(
            TrainingMethod.RandomForest, Stream.of(candidate1, candidate2)
                .map(config -> TunableTrainerConfig.of(config.toMap(), TrainingMethod.valueOf(config.methodName())))
                .collect(Collectors.toList())
        );

        NodeRegressionPipelineTrainConfig trainConfig = NodeRegressionPipelineTrainConfigImpl.builder()
            .username("DUMMY")
            .pipeline("DUMMY")
            .graphName("DUMMY")
            .modelName("DUMMY")
            .targetProperty("target")
            .randomSeed(42L)
            .metrics(List.of(RegressionMetrics.MEAN_SQUARED_ERROR.name()))
            .build();

        var nrTrain = NodeRegressionTrain.create(
            graph,
            List.of("scalar"),
            NodePropertyPredictionSplitConfig.DEFAULT_CONFIG,
            modelCandidates,
            2,
            trainConfig,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        NodeRegressionTrainResult result = nrTrain.compute();
        var trainingStatistics = result.trainingStatistics();

        assertThat(result.regressor()).isInstanceOf(RandomForestRegressor.class);
        assertThat(trainingStatistics.bestParameters().toMap()).isEqualTo(candidate1.toMap());

        var bestMetricData = trainingStatistics.metricsForWinningModel().get(RegressionMetrics.MEAN_SQUARED_ERROR);

        assertThat(bestMetricData.outerTrain()).isEqualTo(21.36533, Offset.offset(1e-5));
        assertThat(bestMetricData.test()).isEqualTo(1056.95979,  Offset.offset(1e-5));
    }

    // TODO more tests: autotuning, mixed candidate space, multiple evaluation metrics, progress logging
}
