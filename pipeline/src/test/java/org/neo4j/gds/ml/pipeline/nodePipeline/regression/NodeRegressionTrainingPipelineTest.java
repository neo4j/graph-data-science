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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.TestGdsCallableFinder;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.ml_api.TrainingMethod;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class NodeRegressionTrainingPipelineTest {

    @Test
    void canCreateEmptyPipeline() {
        var pipeline = new NodeRegressionTrainingPipeline();

        assertThat(pipeline)
            .returns(List.of(), NodeRegressionTrainingPipeline::featureSteps)
            .returns(List.of(), NodeRegressionTrainingPipeline::nodePropertySteps)
            .returns(NodePropertyPredictionSplitConfig.DEFAULT_CONFIG, NodeRegressionTrainingPipeline::splitConfig);

        assertThat(pipeline.trainingParameterSpace())
            .isEqualTo(Map.of(TrainingMethod.RandomForestRegression, List.of(), TrainingMethod.LinearRegression, List.of()));
    }

    @Test
    void canSelectFeature() {
        var pipeline = new NodeRegressionTrainingPipeline();
        var fooStep = new NodeFeatureStep("foo");
        pipeline.addFeatureStep(fooStep);

        assertThat(pipeline)
            .returns(List.of(fooStep), NodeRegressionTrainingPipeline::featureSteps);

        var barStep = new NodeFeatureStep("bar");
        pipeline.addFeatureStep(barStep);

        assertThat(pipeline)
            .returns(List.of(fooStep, barStep), NodeRegressionTrainingPipeline::featureSteps);
    }

    @Test
    void canAddNodePropertySteps() {
        var pipeline = new NodeRegressionTrainingPipeline();
        var pageRankPropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop1")
        );

        pipeline.addNodePropertyStep(pageRankPropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep), NodeRegressionTrainingPipeline::nodePropertySteps);

        var degreeNodePropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop2")
        );

        pipeline.addNodePropertyStep(degreeNodePropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep, degreeNodePropertyStep), NodeRegressionTrainingPipeline::nodePropertySteps);
    }

    @Test
    void addCandidates() {
        var lrConfig = LinearRegressionTrainConfig.of(Map.of("penalty", 19));
        var rfConfig = RandomForestRegressorTrainerConfig.of(Map.of("maxDepth", 19));

        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.addTrainerConfig(lrConfig.toTunableConfig());
        pipeline.addTrainerConfig(rfConfig.toTunableConfig());

        assertThat(pipeline.trainingParameterSpace())
            .usingRecursiveComparison()
            .isEqualTo(
                Map.of(
                    TrainingMethod.LinearRegression, List.of(lrConfig.toTunableConfig()),
                    TrainingMethod.RandomForestRegression, List.of(rfConfig.toTunableConfig())
                ));
    }

    @Test
    void addMultipleCandidates() {
        var config1 = LinearRegressionTrainConfig.of(Map.of("penalty", 19));
        var config2 = LinearRegressionTrainConfig.of(Map.of("penalty", 1337));
        var config3 = LinearRegressionTrainConfig.of(Map.of("penalty", 42));

        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.addTrainerConfig(config1);
        pipeline.addTrainerConfig(config2);
        pipeline.addTrainerConfig(config3);

        var parameterSpace = pipeline.trainingParameterSpace();
        assertThat(parameterSpace.get(TrainingMethod.LinearRegression)).containsExactly(
            config1.toTunableConfig(),
            config2.toTunableConfig(),
            config3.toTunableConfig()
        );
    }

    @Test
    void canSetSplitConfig() {
        var pipeline = new NodeRegressionTrainingPipeline();
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.555).build();
        pipeline.setSplitConfig(splitConfig);

        assertThat(pipeline)
            .returns(splitConfig, NodeRegressionTrainingPipeline::splitConfig);
    }

    @Test
    void overridesTheSplitConfig() {
        var pipeline = new NodeRegressionTrainingPipeline();
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.5).build();
        pipeline.setSplitConfig(splitConfig);

        var splitConfigOverride = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.7).build();
        pipeline.setSplitConfig(splitConfigOverride);

        assertThat(pipeline)
            .returns(splitConfigOverride, NodeRegressionTrainingPipeline::splitConfig);
    }

    @Nested
    class ToMapTest {

        @Test
        void returnsCorrectDefaultsMap() {
            var pipeline = new NodeRegressionTrainingPipeline();
            assertThat(pipeline.toMap()).isEqualTo(Map.of(
                "featurePipeline", Map.of(
                    "nodePropertySteps", List.of(),
                    "featureProperties", List.of()
                ),
                "splitConfig", NodePropertyPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                "trainingParameterSpace", Map.of(
                    TrainingMethod.LinearRegression.toString(), List.of(),
                    TrainingMethod.RandomForestRegression.toString(), List.of()
                ),
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap()
            ));
        }

        @Test
        void returnsCorrectMapWithFullConfiguration() {
            var pipeline = new NodeRegressionTrainingPipeline();
            var nodePropertyStep = new NodePropertyStep(
                TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
                Map.of("mutateProperty", "prop1")
            );
            pipeline.addNodePropertyStep(nodePropertyStep);

            var fooStep = new NodeFeatureStep("foo");
            pipeline.addFeatureStep(fooStep);

            List<TrainerConfig> candidates = List.of(
                LinearRegressionTrainConfig.of(Map.of("penalty", 1000000)),
                LinearRegressionTrainConfig.of(Map.of("penalty", 1))
            );

            candidates.forEach(pipeline::addTrainerConfig);

            var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            assertThat(pipeline.toMap()).isEqualTo(Map.of(
                "featurePipeline", Map.of(
                    "nodePropertySteps", List.of(nodePropertyStep.toMap()),
                    "featureProperties", List.of(fooStep.toMap())
                ),
                "splitConfig", splitConfig.toMap(),
                "trainingParameterSpace", Map.of(
                    TrainingMethod.LinearRegression.toString(), candidates.stream()
                        .map(TrainerConfig::toTunableConfig)
                        .map(TunableTrainerConfig::toMap)
                        .collect(Collectors.toList()),
                    TrainingMethod.RandomForestRegression.toString(), List.of()
                ),
                "autoTuningConfig", AutoTuningConfig.DEFAULT_CONFIG.toMap()
            ));
        }
    }
}
