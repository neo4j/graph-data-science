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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.domain.services.GloballyScopedDependenciesBuilder;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.pipeline.RemoteNodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PipelineApplicationsTest {

    private static final String REMOTE_PROC_NAME = "gds.remote.mutate";

    @Test
    void shouldUseFactoryToConstructRegressionComputation() {
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .databaseId(DatabaseId.DEFAULT)
            .user(User.DEFAULT)
            .build();
        var pipelineConfigurationParser = new PipelineConfigurationParser(requestScopedDependencies.user());
        var algorithmProcessingTemplate = mock(AlgorithmProcessingTemplate.class);
        var nodeRegressionTrainComputationFactory = mock(NodeRegressionTrainComputationFactory.class);
        var pipelineApplications = new PipelineApplications(
            null,
            new GloballyScopedDependenciesBuilder().with(ModelCatalog.EMPTY).build(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            requestScopedDependencies.user(),
            pipelineConfigurationParser,
            null,
            null,
            null,
            null,
            null,
            null,
            algorithmProcessingTemplate,
            null,
            null,
            nodeRegressionTrainComputationFactory,
            RemoteNodePropertyStepExecutor.EMPTY
        );

        var configuration = Map.of(
            "graphName",
            "some graph name",
            "metrics",
            List.of(RegressionMetrics.values()[0].name()),
            "modelName",
            "some model name",
            "pipeline",
            "some pipeline",
            "targetProperty",
            "some target property"
        );
        @SuppressWarnings("unchecked") Computation<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> computation = mock(Computation.class);
        when(nodeRegressionTrainComputationFactory.create(any())).thenReturn(computation);
        pipelineApplications.nodeRegressionTrain(null, configuration);

        verify(algorithmProcessingTemplate).processAlgorithmAndAnySideEffects(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(computation),
            any(),
            any()
        );
    }

    @Test
    void shouldAddRemoteNodePropertyStepToNodeClassificationPipeline() {
        var repository = new PipelineRepository();
        var applications = pipelineApplications(repository, FakeRemoteExecutor.handling(REMOTE_PROC_NAME));
        var pipelineName = PipelineName.parse("nc-" + UUID.randomUUID());
        applications.createNodeClassificationTrainingPipeline(pipelineName);

        applications.addNodePropertyToNodeClassificationPipeline(
            pipelineName,
            "remote",
            Map.of("mutateProperty", "embedding")
        );

        var pipeline = repository.getNodeClassificationTrainingPipeline(User.DEFAULT, pipelineName);
        assertThat(pipeline.nodePropertySteps()).hasSize(1);
        assertThat(pipeline.nodePropertySteps().get(0).procName()).isEqualTo(REMOTE_PROC_NAME);
        assertThat(pipeline.nodePropertySteps().get(0).mutateNodeProperty()).isEqualTo("embedding");
    }

    @Test
    void shouldAddRemoteNodePropertyStepToNodeRegressionPipeline() {
        var repository = new PipelineRepository();
        var applications = pipelineApplications(repository, FakeRemoteExecutor.handling(REMOTE_PROC_NAME));
        var pipelineName = PipelineName.parse("nr-" + UUID.randomUUID());
        applications.createNodeRegressionTrainingPipeline(pipelineName);

        applications.addNodePropertyToNodeRegressionPipeline(
            pipelineName,
            "remote",
            Map.of("mutateProperty", "embedding")
        );

        var pipeline = repository.getNodeRegressionTrainingPipeline(User.DEFAULT, pipelineName);
        assertThat(pipeline.nodePropertySteps()).hasSize(1);
        assertThat(pipeline.nodePropertySteps().get(0).mutateNodeProperty()).isEqualTo("embedding");
    }

    @Test
    void shouldAddRemoteNodePropertyStepToLinkPredictionPipeline() {
        var repository = new PipelineRepository();
        var applications = pipelineApplications(repository, FakeRemoteExecutor.handling(REMOTE_PROC_NAME));
        var pipelineName = PipelineName.parse("lp-" + UUID.randomUUID());
        applications.createLinkPredictionTrainingPipeline(pipelineName);

        applications.addNodePropertyToLinkPredictionPipeline(
            pipelineName,
            "remote",
            Map.of("mutateProperty", "embedding")
        );

        var pipeline = repository.getLinkPredictionTrainingPipeline(User.DEFAULT, pipelineName);
        assertThat(pipeline.nodePropertySteps()).hasSize(1);
        assertThat(pipeline.nodePropertySteps().get(0).mutateNodeProperty()).isEqualTo("embedding");
    }

    private static PipelineApplications pipelineApplications(
        PipelineRepository repository,
        RemoteNodePropertyStepExecutor remoteNodePropertyStepExecutor
    ) {
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .databaseId(DatabaseId.DEFAULT)
            .user(User.DEFAULT)
            .build();
        return new PipelineApplications(
            null,
            new GloballyScopedDependenciesBuilder().with(ModelCatalog.EMPTY).build(),
            null,
            null,
            repository,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            requestScopedDependencies.user(),
            new PipelineConfigurationParser(requestScopedDependencies.user()),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            remoteNodePropertyStepExecutor
        );
    }

    private static final class FakeRemoteExecutor implements RemoteNodePropertyStepExecutor {
        private final Set<String> handledProcedures;

        private FakeRemoteExecutor(Set<String> handledProcedures) {
            this.handledProcedures = handledProcedures;
        }

        static FakeRemoteExecutor handling(String procName) {
            return new FakeRemoteExecutor(Set.of(procName));
        }

        @Override
        public boolean handles(String procName) {
            return handledProcedures.contains(procName);
        }

        @Override
        public void execute(
            String procName,
            String graphName,
            Map<String, Object> configuration,
            List<String> nodeLabels,
            List<String> relationshipTypes,
            TerminationMonitor terminationMonitor
        ) {
            throw new UnsupportedOperationException();
        }
    }
}
