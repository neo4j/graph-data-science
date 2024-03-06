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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfigImpl;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.TestSupport.assertMemoryEstimation;

class NodeClassificationTrainMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @MethodSource("trainerMethodConfigs")
    void shouldEstimateMemory(List<TunableTrainerConfig> tunableConfigs, MemoryRange memoryRange) {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc", Map.of("mutateProperty", "myNewProp"))
        );
        pipeline.featureProperties().addAll(List.of("array", "scalar", "pr"));

        for (TunableTrainerConfig tunableConfig : tunableConfigs) {
            pipeline.addTrainerConfig(tunableConfig);
        }

        // Limit maxTrials to make comparison with concrete-only parameter spaces easier.
        pipeline.setAutoTuningConfig(AutoTuningConfigImpl.builder().maxTrials(2).build());

        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("")
            .modelUser("myUser")
            .graphName("foo")
            .modelName("myModel")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .relationshipTypes(List.of("SOME_REL"))
            .targetNodeLabels(List.of("SOME_LABEL"))
            .metrics(List.of(ClassificationMetricSpecification.Parser.parse("F1_WEIGHTED")))
            .build();

        var memoryEstimation = new NodeClassificationTrainMemoryEstimateDefinition(pipeline, config, new OpenModelCatalog()).memoryEstimation();

        // TODO: replace this with proper asserts
        assertMemoryEstimation(
            () -> memoryEstimation,
            9,
            7,
            config.concurrency(),
            memoryRange
        );
    }

    private static Stream<Arguments> trainerMethodConfigs() {
        return Stream.of(
            Arguments.of(
                List.of(LogisticRegressionTrainConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(778_968, 810_928)
            ),
            Arguments.of(
                List.of(RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()),
                MemoryRange.of(90_906, 207_678)
            ),
            Arguments.of(
                List.of(
                    LogisticRegressionTrainConfig.DEFAULT.toTunableConfig(),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(859_936, 927_176)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("penalty", Map.of("range", List.of(1e-4, 1e4))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(939_936, 1_007_176)
            ),
            Arguments.of(
                List.of(
                    TunableTrainerConfig.of(
                        Map.of("batchSize", Map.of("range", List.of(1, 100_000))),
                        TrainingMethod.LogisticRegression
                    ),
                    RandomForestClassifierTrainerConfig.DEFAULT.toTunableConfig()
                ),
                MemoryRange.of(430_110_336, 430_177_576)
            )
        );
    }


}
