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

import org.junit.jupiter.api.DynamicTest;
import org.neo4j.gds.core.model.CatalogModelContainer;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyTrainingPipeline;
import org.neo4j.gds.termination.TerminatedException;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public interface PipelineTrainAlgorithmTest {

    static DynamicTest terminationFlagTest(
        PipelineTrainAlgorithm<?, ?, ?, ?> algorithm
    ) {
        return DynamicTest.dynamicTest("terminationFlag", () ->
            assertThatThrownBy(() -> {
                algorithm.setTerminationFlag(() -> false);
                algorithm.compute();
            })
                .isInstanceOf(TerminatedException.class)
                .hasMessageContaining("The execution has been terminated.")
        );
    }

    static DynamicTest trainsAModel(
        PipelineTrainAlgorithm<?, ?, ?, ?> algorithm,
        String expectedType
    ) {
        return DynamicTest.dynamicTest(
            "trainsAModel",
            () -> assertThat(algorithm.compute().model().algoType()).isEqualTo(expectedType)
        );
    }

    static <MODEL_RESULT extends CatalogModelContainer<?, ?, ?>> DynamicTest originalSchemaTest(
        PipelineTrainAlgorithm<?, MODEL_RESULT, ?, ?> algorithm,
        TrainingPipeline<?> pipeline
    ) {
        return DynamicTest.dynamicTest("originalSchema", () -> {
            MODEL_RESULT modelResult = algorithm.compute();
            var schema = modelResult.model().graphSchema();
            var nodeProperties = schema.nodeSchema().allProperties();
            var pipeNodeProperties = pipeline.nodePropertySteps
                .stream()
                .map(ExecutableNodePropertyStep::mutateNodeProperty)
                .collect(Collectors.toList());

            assertThat(pipeNodeProperties).isNotEmpty();
            assertThat(nodeProperties).doesNotContainAnyElementsOf(pipeNodeProperties);

        });
    }

    static <PIPELINE extends NodePropertyTrainingPipeline> DynamicTest testParameterSpaceValidation(
        Function<PIPELINE, PipelineTrainAlgorithm<?, ?, ?, ?>> algorithmSupplier,
        PIPELINE pipeline
    ) {
        return DynamicTest.dynamicTest("testParameterSpaceValidation", () -> {
                pipeline.featureProperties().addAll(List.of("array", "scalar"));
                var algorithm = algorithmSupplier.apply(pipeline);

                assertThatThrownBy(algorithm::compute).hasMessageContaining("Need at least one model candidate for training.");
            }
        );
    }
}
