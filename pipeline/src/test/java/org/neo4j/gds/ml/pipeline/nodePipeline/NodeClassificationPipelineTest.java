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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.ml.pipeline.TestGdsCallableFinder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationPipelineTest {

    @Test
    void canCreateEmptyPipeline() {
        var pipeline = new NodeClassificationPipeline();

        assertThat(pipeline)
            .returns(List.of(), NodeClassificationPipeline::featureSteps)
            .returns(List.of(), NodeClassificationPipeline::nodePropertySteps)
            .returns(NodeClassificationSplitConfig.DEFAULT_CONFIG, NodeClassificationPipeline::splitConfig);

        assertThat(pipeline.trainingParameterSpace())
            .usingRecursiveComparison()
            .isEqualTo(List.of(LogisticRegressionTrainConfig.defaultConfig()));
    }

    @Test
    void canSelectFeature() {
        var pipeline = new NodeClassificationPipeline();
        var fooStep = new NodeClassificationFeatureStep("foo");
        pipeline.addFeatureStep(fooStep);

        assertThat(pipeline)
            .returns(List.of(fooStep), NodeClassificationPipeline::featureSteps);

        var barStep = new NodeClassificationFeatureStep("bar");
        pipeline.addFeatureStep(barStep);

        assertThat(pipeline)
            .returns(List.of(fooStep, barStep), NodeClassificationPipeline::featureSteps);
    }

    @Test
    void canAddNodePropertySteps() {
        var pipeline = new NodeClassificationPipeline();
        var pageRankPropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop1")
        );

        pipeline.addNodePropertyStep(pageRankPropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep), NodeClassificationPipeline::nodePropertySteps);

        var degreeNodePropertyStep = new NodePropertyStep(
            TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
            Map.of("mutateProperty", "prop2")
        );

        pipeline.addNodePropertyStep(degreeNodePropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep, degreeNodePropertyStep), NodeClassificationPipeline::nodePropertySteps);
    }

    @Test
    void canSetParameterSpace() {
        var config = LogisticRegressionTrainConfig.of(Map.of("penalty", 19));

        var pipeline = new NodeClassificationPipeline();
        pipeline.setTrainingParameterSpace(List.of(
            config
        ));

        assertThat(pipeline.trainingParameterSpace()).containsExactly(config);
    }

    @Test
    void overridesTheParameterSpace() {
        var config1 = LogisticRegressionTrainConfig.of(Map.of("penalty", 19));
        var config2 = LogisticRegressionTrainConfig.of(Map.of("penalty", 1337));
        var config3 = LogisticRegressionTrainConfig.of(Map.of("penalty", 42));

        var pipeline = new NodeClassificationPipeline();
        pipeline.setTrainingParameterSpace(List.of(
            config1
        ));
        pipeline.setTrainingParameterSpace(List.of(
            config2,
            config3
        ));

        var parameterSpace = pipeline.trainingParameterSpace();
        assertThat(parameterSpace).containsExactly(config2, config3);
    }

    @Test
    void canSetSplitConfig() {
        var pipeline = new NodeClassificationPipeline();
        var splitConfig = NodeClassificationSplitConfig.testBuilder().testFraction(0.555).build();
        pipeline.setSplitConfig(splitConfig);

        assertThat(pipeline)
            .returns(splitConfig, NodeClassificationPipeline::splitConfig);
    }

    @Test
    void overridesTheSplitConfig() {
        var pipeline = new NodeClassificationPipeline();
        var splitConfig = NodeClassificationSplitConfig.testBuilder().testFraction(0.5).build();
        pipeline.setSplitConfig(splitConfig);

        var splitConfigOverride = NodeClassificationSplitConfig.testBuilder().testFraction(0.7).build();
        pipeline.setSplitConfig(splitConfigOverride);

        assertThat(pipeline)
            .returns(splitConfigOverride, NodeClassificationPipeline::splitConfig);
    }

    @Nested
    class ToMapTest {

        @Test
        void returnsCorrectDefaultsMap() {
            var pipeline = new NodeClassificationPipeline();
            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace")
                .satisfies(pipelineMap -> assertThat(pipelineMap.get("featurePipeline"))
                    .isInstanceOf(Map.class)
                    .asInstanceOf(InstanceOfAssertFactories.MAP)
                    .containsOnlyKeys("nodePropertySteps", "featureProperties")
                    .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("nodePropertySteps"))
                    .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("featureProperties")))
                .returns(
                    NodeClassificationSplitConfig.DEFAULT_CONFIG.toMap(),
                    pipelineMap -> pipelineMap.get("splitConfig")
                )
                .returns(
                    List.of(LogisticRegressionTrainConfig.defaultConfig().toMap()),
                    pipelineMap -> pipelineMap.get("trainingParameterSpace")
                );
        }

        @Test
        void returnsCorrectMapWithFullConfiguration() {
            var pipeline = new NodeClassificationPipeline();
            var nodePropertyStep = new NodePropertyStep(
                TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
                Map.of("mutateProperty", "prop1")
            );
            pipeline.addNodePropertyStep(nodePropertyStep);

            var fooStep = new NodeClassificationFeatureStep("foo");
            pipeline.addFeatureStep(fooStep);

            pipeline.setTrainingParameterSpace(List.of(
                LogisticRegressionTrainConfig.of(Map.of("penalty", 1000000)),
                LogisticRegressionTrainConfig.of(Map.of("penalty", 1))
            ));

            var splitConfig = NodeClassificationSplitConfig.testBuilder().testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace")
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
                    pipeline.trainingParameterSpace()
                        .stream()
                        .map(LogisticRegressionTrainConfig::toMap)
                        .collect(Collectors.toList()),
                    pipelineMap -> pipelineMap.get("trainingParameterSpace")
                );
        }
    }

    @Nested
    class CopyPipelineTest {

        @Test
        void deepCopiesFeatureSteps() {
            var pipeline = new NodeClassificationPipeline();
            var fooStep = new NodeClassificationFeatureStep("foo");
            pipeline.addFeatureStep(fooStep);

            var copy = pipeline.copy();
            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> assertThat(copiedPipeline.featureSteps())
                    .isNotSameAs(pipeline.featureSteps())
                    .containsExactly(fooStep));

            var barStep = new NodeClassificationFeatureStep("bar");
            pipeline.addFeatureStep(barStep);

            assertThat(copy.featureSteps()).doesNotContain(barStep);
        }

        @Test
        void deepCopiesNodePropertySteps() {
            var pipeline = new NodeClassificationPipeline();
            var nodePropertyStep = new NodePropertyStep(
                TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
                Map.of("mutateProperty", "prop1")
            );
            pipeline.addNodePropertyStep(nodePropertyStep);

            var copy = pipeline.copy();
            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> assertThat(copiedPipeline.nodePropertySteps())
                    .isNotSameAs(pipeline.nodePropertySteps())
                    .containsExactly(nodePropertyStep));

            var otherNodePropertyStep = new NodePropertyStep(
                TestGdsCallableFinder.findByName("gds.testProc.mutate").orElseThrow(),
                Map.of("mutateProperty", "prop2")
            );
            pipeline.addNodePropertyStep(otherNodePropertyStep);

            assertThat(copy.nodePropertySteps()).doesNotContain(otherNodePropertyStep);
        }

        @Test
        void deepCopiesParameterSpace() {
            var pipeline = new NodeClassificationPipeline();
            pipeline.setTrainingParameterSpace(List.of(
                LogisticRegressionTrainConfig.of(Map.of("penalty", 1000000)),
                LogisticRegressionTrainConfig.of(Map.of("penalty", 1))
            ));

            var copy = pipeline.copy();

            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> {
                    var copiedParameterSpace = copiedPipeline.trainingParameterSpace();
                    var originalParameterSpace = pipeline.trainingParameterSpace();
                    assertThat(copiedParameterSpace)
                        // Look at the pipeline because there are some defaults are added behind the scene.
                        .isNotSameAs(originalParameterSpace)
                        .containsExactlyInAnyOrderElementsOf(originalParameterSpace);
                });
        }

        @Test
        void doesntDeepCopySplitConfig() {
            var pipeline = new NodeClassificationPipeline();
            var splitConfig = NodeClassificationSplitConfig.testBuilder().testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            var copy = pipeline.copy();

            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> assertThat(copiedPipeline.splitConfig()).isSameAs(splitConfig));
        }
    }
}
