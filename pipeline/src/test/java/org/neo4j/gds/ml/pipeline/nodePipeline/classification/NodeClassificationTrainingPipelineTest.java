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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.TestGdsCallableFinder;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationTrainingPipelineTest {

    @Test
    void canCreateEmptyPipeline() {
        var pipeline = new NodeClassificationTrainingPipeline();

        assertThat(pipeline)
            .returns(List.of(), NodeClassificationTrainingPipeline::featureSteps)
            .returns(List.of(), NodeClassificationTrainingPipeline::nodePropertySteps)
            .returns(NodePropertyPredictionSplitConfig.DEFAULT_CONFIG, NodeClassificationTrainingPipeline::splitConfig);

        assertThat(pipeline.trainingParameterSpace())
            .isEqualTo(Map.of(TrainingMethod.LogisticRegression, List.of(), TrainingMethod.RandomForestClassification, List.of(), TrainingMethod.MLPClassification, List.of()));
    }

    @Test
    void canSelectFeature() {
        var pipeline = new NodeClassificationTrainingPipeline();
        var fooStep = new NodeFeatureStep("foo");
        pipeline.addFeatureStep(fooStep);

        assertThat(pipeline)
            .returns(List.of(fooStep), NodeClassificationTrainingPipeline::featureSteps);

        var barStep = new NodeFeatureStep("bar");
        pipeline.addFeatureStep(barStep);

        assertThat(pipeline)
            .returns(List.of(fooStep, barStep), NodeClassificationTrainingPipeline::featureSteps);
    }

    @Test
    void canAddNodePropertySteps() {
        var pipeline = new NodeClassificationTrainingPipeline();
        var pageRankPropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop1")
        );

        pipeline.addNodePropertyStep(pageRankPropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep), NodeClassificationTrainingPipeline::nodePropertySteps);

        var degreeNodePropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop2")
        );

        pipeline.addNodePropertyStep(degreeNodePropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep, degreeNodePropertyStep), NodeClassificationTrainingPipeline::nodePropertySteps);
    }

    @Test
    void canSetParameterSpace() {
        var config = LogisticRegressionTrainConfig.of(Map.of("penalty", 19));

        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addTrainerConfig(config);

        assertThat(pipeline
            .trainingParameterSpace()
            .get(TrainingMethod.LogisticRegression)).containsExactly(config.toTunableConfig());
    }

    @Test
    void addMultipleCandidates() {
        var config1 = LogisticRegressionTrainConfig.of(Map.of("penalty", 19));
        var config2 = LogisticRegressionTrainConfig.of(Map.of("penalty", 1337));
        var config3 = LogisticRegressionTrainConfig.of(Map.of("penalty", 42));

        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.addTrainerConfig(config1);
        pipeline.addTrainerConfig(config2);
        pipeline.addTrainerConfig(config3);

        var parameterSpace = pipeline.trainingParameterSpace();
        assertThat(parameterSpace.get(TrainingMethod.LogisticRegression)).containsExactly(
            config1.toTunableConfig(),
            config2.toTunableConfig(),
            config3.toTunableConfig()
        );
    }

    @Test
    void canSetSplitConfig() {
        var pipeline = new NodeClassificationTrainingPipeline();
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.555).build();
        pipeline.setSplitConfig(splitConfig);

        assertThat(pipeline)
            .returns(splitConfig, NodeClassificationTrainingPipeline::splitConfig);
    }

    @Test
    void overridesTheSplitConfig() {
        var pipeline = new NodeClassificationTrainingPipeline();
        var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.5).build();
        pipeline.setSplitConfig(splitConfig);

        var splitConfigOverride = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.7).build();
        pipeline.setSplitConfig(splitConfigOverride);

        assertThat(pipeline)
            .returns(splitConfigOverride, NodeClassificationTrainingPipeline::splitConfig);
    }

    @Nested
    class ToMapTest {

        @Test
        void returnsCorrectDefaultsMap() {
            var pipeline = new NodeClassificationTrainingPipeline();
            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace", "autoTuningConfig")
                .satisfies(pipelineMap -> assertThat(pipelineMap.get("featurePipeline"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsOnlyKeys("nodePropertySteps", "featureProperties")
                    .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("nodePropertySteps"))
                    .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("featureProperties")))
                .returns(
                    NodePropertyPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                    pipelineMap -> pipelineMap.get("splitConfig")
                )
                .returns(
                    Map.of(TrainingMethod.LogisticRegression.toString(), List.of(), TrainingMethod.RandomForestClassification.toString(), List.of(), TrainingMethod.MLPClassification.toString(), List.of()),
                    pipelineMap -> pipelineMap.get("trainingParameterSpace")
                )
                .returns(
                    AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                    pipelineMap -> pipelineMap.get("autoTuningConfig")
                );
        }

        @Test
        void returnsCorrectMapWithFullConfiguration() {
            var pipeline = new NodeClassificationTrainingPipeline();
            var nodePropertyStep = new NodePropertyStep(
                TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
                Map.of("mutateProperty", "prop1")
            );
            pipeline.addNodePropertyStep(nodePropertyStep);

            var fooStep = new NodeFeatureStep("foo");
            pipeline.addFeatureStep(fooStep);

            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("penalty", 1000000)));
            pipeline.addTrainerConfig(LogisticRegressionTrainConfig.of(Map.of("penalty", 1)));

            var splitConfig = NodePropertyPredictionSplitConfigImpl.builder().testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace", "autoTuningConfig")
                .satisfies(pipelineMap -> assertThat(pipelineMap.get("featurePipeline"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsOnlyKeys("nodePropertySteps", "featureProperties")
                    .returns(
                        List.of(nodePropertyStep.toMap()),
                        featurePipelineMap -> featurePipelineMap.get("nodePropertySteps")
                    )
                    .returns(
                        List.of(fooStep.toMap()),
                        featurePipelineMap -> featurePipelineMap.get("featureProperties")
                    ))
                .returns(
                    pipeline.splitConfig().toMap(),
                    pipelineMap -> pipelineMap.get("splitConfig")
                )
                .returns(
                    pipeline.trainingParameterSpace().get(TrainingMethod.LogisticRegression)
                        .stream()
                        .map(TunableTrainerConfig::toMap)
                        .collect(Collectors.toList()),
                    pipelineMap -> ((Map<String, Object>) pipelineMap.get("trainingParameterSpace")).get(TrainingMethod.LogisticRegression.name())
                ).returns(
                    AutoTuningConfig.DEFAULT_CONFIG.toMap(),
                    pipelineMap -> pipelineMap.get("autoTuningConfig")
                );
        }
    }
}
