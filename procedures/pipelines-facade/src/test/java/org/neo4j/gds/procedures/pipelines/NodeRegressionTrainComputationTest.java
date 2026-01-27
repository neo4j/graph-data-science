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
package org.neo4j.gds.procedures.pipelines;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.executor.MemoryEstimationContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeRegressionTrainComputationTest {
    @Test
    void shouldValidateContextNodeLabelsInNodePropertySteps() {
        var pipelineRepository = mock(PipelineRepository.class);
        var nodeRegressionTrainComputation = constructNodeRegressionTrainComputation(pipelineRepository);

        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
            "degree",
            Map.of("mutateProperty", "pr", "contextNodeLabels", List.of("A"))
        ));
        when(pipelineRepository.getNodeRegressionTrainingPipeline(
            User.DEFAULT,
            PipelineName.parse("some pipeline")
        )).thenReturn(pipeline);
        var graphStore = mock(GraphStore.class);
        when(graphStore.nodeLabels()).thenReturn(Set.of(NodeLabel.of("B"), NodeLabel.of("C")));
        assertThatIllegalArgumentException()
            .isThrownBy(() -> nodeRegressionTrainComputation.compute(null, graphStore))
            .withMessage(
                "Could not find the specified contextNodeLabels for step `gds.degree.mutate` of ['A']. Available labels are ['B', 'C'].");
    }

    @Test
    void shouldValidateContextRelationshipTypesInNodePropertySteps() {
        var pipelineRepository = mock(PipelineRepository.class);
        var nodeRegressionTrainComputation = constructNodeRegressionTrainComputation(pipelineRepository);

        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
            "degree",
            Map.of("mutateProperty", "pr", "contextRelationshipTypes", List.of("A"))
        ));
        when(pipelineRepository.getNodeRegressionTrainingPipeline(
            User.DEFAULT,
            PipelineName.parse("some pipeline")
        )).thenReturn(pipeline);
        var graphStore = mock(GraphStore.class);
        when(graphStore.relationshipTypes()).thenReturn(Set.of(RelationshipType.of("B"), RelationshipType.of("C")));
        assertThatIllegalArgumentException()
            .isThrownBy(() -> nodeRegressionTrainComputation.compute(null, graphStore))
            .withMessage(
                "Could not find the specified contextRelationshipTypes for step `gds.degree.mutate` of ['A']. Available relationship types are ['B', 'C'].");
    }

    private static NodeRegressionTrainComputation constructNodeRegressionTrainComputation(PipelineRepository pipelineRepository) {
        Map<String, Object> rawConfiguration = Map.of(
            "graphName", "some graph name",
            "metrics", List.of(RegressionMetrics.values()[0].name()),
            "modelName", "some model name",
            "pipeline", "some pipeline",
            "targetProperty", "some target property"
        );

        var configuration = NodeRegressionPipelineTrainConfig.of(
            User.DEFAULT.getUsername(),
            CypherMapWrapper.create(rawConfiguration)
        );

        // all these fakes just to satisfy null checks, they are never used :facepalm:
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .userLogRegistry(UserLogRegistry.EMPTY)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(null, requestScopedDependencies);

        return new NodeRegressionTrainComputation(
            Log.noOpLog(),
            null,
            pipelineRepository,
            CloseableResourceRegistry.EMPTY,
            DatabaseId.DEFAULT,
            new MemoryEstimationContext(false),
            Metrics.DISABLED,
            NodeLookup.EMPTY,
            null,
            ProcedureReturnColumns.EMPTY,
            null,
            requestScopedDependencies.taskRegistryFactory(),
            TerminationFlag.RUNNING_TRUE,
            TerminationMonitor.EMPTY,
            requestScopedDependencies.userLogRegistry(),
            progressTrackerCreator,
            null,
            configuration
        );
    }
}
