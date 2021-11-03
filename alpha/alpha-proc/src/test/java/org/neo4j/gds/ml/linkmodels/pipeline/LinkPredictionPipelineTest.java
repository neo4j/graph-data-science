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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.CosineFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.linkFeatures.linkfunctions.HadamardFeatureStep;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class LinkPredictionPipelineTest {

    @Test
    void canCreateEmptyPipeline() {
        var pipeline = new LinkPredictionPipeline();

        assertThat(pipeline)
            .returns(List.of(), LinkPredictionPipeline::featureSteps)
            .returns(List.of(), LinkPredictionPipeline::nodePropertySteps)
            .returns(LinkPredictionSplitConfig.DEFAULT_CONFIG, LinkPredictionPipeline::splitConfig);

        assertThat(pipeline.trainingParameterSpace())
            .usingRecursiveComparison()
            .isEqualTo(List.of(LinkLogisticRegressionTrainConfig.defaultConfig()));
    }

    @Test
    void canAddFeatureSteps() {
        var pipeline = new LinkPredictionPipeline();
        var hadamardFeatureStep = new HadamardFeatureStep(List.of("a"));
        pipeline.addFeatureStep(hadamardFeatureStep);

        assertThat(pipeline)
            .returns(List.of(hadamardFeatureStep), LinkPredictionPipeline::featureSteps);

        var cosineFeatureStep = new CosineFeatureStep(List.of("b", "c"));
        pipeline.addFeatureStep(cosineFeatureStep);

        assertThat(pipeline)
            .returns(List.of(hadamardFeatureStep, cosineFeatureStep), LinkPredictionPipeline::featureSteps);
    }

    @Test
    void canAddNodePropertySteps() {
        var pipeline = new LinkPredictionPipeline();
        var pageRankPropertyStep = NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr"));
        pipeline.addNodePropertyStep(pageRankPropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep), LinkPredictionPipeline::nodePropertySteps);

        var degreeNodePropertyStep = NodePropertyStep.of("degree", Map.of("mutateProperty", "degree"));
        pipeline.addNodePropertyStep(degreeNodePropertyStep);

        assertThat(pipeline)
            .returns(List.of(pageRankPropertyStep, degreeNodePropertyStep), LinkPredictionPipeline::nodePropertySteps);
    }

    @Test
    void canSetParameterSpace() {
        var config = LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 19));

        var pipeline = new LinkPredictionPipeline();
        pipeline.setTrainingParameterSpace(List.of(
            config
        ));

        assertThat(pipeline.trainingParameterSpace()).containsExactly(config);
    }

    @Test
    void overridesTheParameterSpace() {
        var config1 = LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 19));
        var config2 = LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1337));
        var config3 = LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 42));

        var pipeline = new LinkPredictionPipeline();
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
        var pipeline = new LinkPredictionPipeline();
        var splitConfig = LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build();
        pipeline.setSplitConfig(splitConfig);

        assertThat(pipeline)
            .returns(splitConfig, LinkPredictionPipeline::splitConfig);
    }

    @Test
    void overridesTheSplitConfig() {
        var pipeline = new LinkPredictionPipeline();
        var splitConfig = LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build();
        pipeline.setSplitConfig(splitConfig);

        var splitConfigOverride = LinkPredictionSplitConfig.builder().trainFraction(0.1).testFraction(0.7).build();
        pipeline.setSplitConfig(splitConfigOverride);

        assertThat(pipeline)
            .returns(splitConfigOverride, LinkPredictionPipeline::splitConfig);
    }

    @Nested
    class ToMapTest {

        @Test
        void returnsCorrectDefaultsMap() {
            var pipeline = new LinkPredictionPipeline();
            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace")
                .satisfies(pipelineMap -> {
                    assertThat(pipelineMap.get("featurePipeline"))
                        .isInstanceOf(Map.class)
                        .asInstanceOf(InstanceOfAssertFactories.MAP)
                        .containsOnlyKeys("nodePropertySteps", "featureSteps")
                        .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("nodePropertySteps"))
                        .returns(List.of(), featurePipelineMap -> featurePipelineMap.get("featureSteps"));
                })
                .returns(
                    LinkPredictionSplitConfig.DEFAULT_CONFIG.toMap(),
                    pipelineMap -> pipelineMap.get("splitConfig")
                )
                .returns(
                    List.of(LinkLogisticRegressionTrainConfig.defaultConfig().toMap()),
                    pipelineMap -> pipelineMap.get("trainingParameterSpace")
                );
        }

        @Test
        void returnsCorrectMapWithFullConfiguration() {
            var pipeline = new LinkPredictionPipeline();
            var pageRankPropertyStep = NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr"));
            pipeline.addNodePropertyStep(pageRankPropertyStep);

            var hadamardFeatureStep = new HadamardFeatureStep(List.of("a"));
            pipeline.addFeatureStep(hadamardFeatureStep);

            pipeline.setTrainingParameterSpace(List.of(
                LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1000000)),
                LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1))
            ));

            var splitConfig = LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            assertThat(pipeline.toMap())
                .containsOnlyKeys("featurePipeline", "splitConfig", "trainingParameterSpace")
                .satisfies(pipelineMap -> {
                    assertThat(pipelineMap.get("featurePipeline"))
                        .isInstanceOf(Map.class)
                        .asInstanceOf(InstanceOfAssertFactories.MAP)
                        .containsOnlyKeys("nodePropertySteps", "featureSteps")
                        .returns(
                            List.of(pageRankPropertyStep.toMap()),
                            featurePipelineMap -> featurePipelineMap.get("nodePropertySteps")
                        )
                        .returns(
                            List.of(hadamardFeatureStep.toMap()),
                            featurePipelineMap -> featurePipelineMap.get("featureSteps")
                        );
                })
                .returns(
                    pipeline.splitConfig().toMap(),
                    pipelineMap -> pipelineMap.get("splitConfig")
                )
                .returns(
                    pipeline.trainingParameterSpace().stream().map(LinkLogisticRegressionTrainConfig::toMap).collect(Collectors.toList()),
                    pipelineMap -> pipelineMap.get("trainingParameterSpace")
                );
        }
    }

    @Nested
    class CopyPipelineTest {

        @Test
        void deepCopiesFeatureSteps() {
            var pipeline = new LinkPredictionPipeline();
            var hadamardFeatureStep = new HadamardFeatureStep(List.of("a"));
            pipeline.addFeatureStep(hadamardFeatureStep);

            var copy = pipeline.copy();
            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> assertThat(copiedPipeline.featureSteps())
                    .isNotSameAs(pipeline.featureSteps())
                    .containsExactly(hadamardFeatureStep));

            var cosineFeatureStep = new CosineFeatureStep(List.of("b", "c"));
            pipeline.addFeatureStep(cosineFeatureStep);

            assertThat(copy.featureSteps()).doesNotContain(cosineFeatureStep);
        }

        @Test
        void deepCopiesNodePropertySteps() {
            var pipeline = new LinkPredictionPipeline();
            var pageRankPropertyStep = NodePropertyStep.of("pageRank", Map.of("mutateProperty", "pr"));
            pipeline.addNodePropertyStep(pageRankPropertyStep);

            var copy = pipeline.copy();
            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> assertThat(copiedPipeline.nodePropertySteps())
                    .isNotSameAs(pipeline.nodePropertySteps())
                    .containsExactly(pageRankPropertyStep));

            var degreeNodePropertyStep = NodePropertyStep.of("degree", Map.of("mutateProperty", "degree"));
            pipeline.addNodePropertyStep(degreeNodePropertyStep);

            assertThat(copy.nodePropertySteps()).doesNotContain(degreeNodePropertyStep);
        }

        @Test
        void deepCopiesParameterSpace() {
            var pipeline = new LinkPredictionPipeline();
            pipeline.setTrainingParameterSpace(List.of(
                LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1000000)),
                LinkLogisticRegressionTrainConfig.of(4, Map.of("penalty", 1))
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
            var pipeline = new LinkPredictionPipeline();
            var splitConfig = LinkPredictionSplitConfig.builder().trainFraction(0.01).testFraction(0.5).build();
            pipeline.setSplitConfig(splitConfig);

            var copy = pipeline.copy();

            assertThat(copy)
                .isNotSameAs(pipeline)
                .satisfies(copiedPipeline -> {
                    assertThat(copiedPipeline.splitConfig()).isSameAs(splitConfig);
                });
        }
    }
}
