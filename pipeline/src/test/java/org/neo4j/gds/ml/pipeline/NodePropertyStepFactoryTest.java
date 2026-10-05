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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_NODE_LABELS;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_RELATIONSHIP_TYPES;

class NodePropertyStepFactoryTest {

    private static final String REMOTE_PROC_NAME = "gds.remote.mutate";

    @Test
    void shouldRejectFastPathAsNodePropertyStepWithSaneError() {
        // FastPath exists as a standalone procedure but is intentionally not supported in pipelines.
        // Someone trying to add it as a node-property step must get a clear, actionable error rather
        // than the generic "could not find a procedure" (the standalone proc does exist).
        assertThatThrownBy(() -> NodePropertyStepFactory.createNodePropertyStep("fastPath", Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("gds.fastpath.mutate")
            .hasMessageContaining("not supported as a node property step in training pipelines");
    }

    @Test
    void shouldRejectSupervisedGraphSageAsNodePropertyStepWithSaneError() {
        assertThatThrownBy(() -> NodePropertyStepFactory.createNodePropertyStep("graphSage.supervised", Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("gds.graphsage.supervised.mutate")
            .hasMessageContaining("not supported as a node property step in training pipelines");
    }

    @Test
    void shouldCreateRemoteStepForHandledProcedure() {
        var executor = FakeRemoteExecutor.handling(REMOTE_PROC_NAME);
        var config = Map.<String, Object>of("modelName", "myModel", "mutateProperty", "embedding");

        var step = NodePropertyStepFactory.createNodePropertyStep("remote", config, executor);

        assertThat(step).isInstanceOf(RemoteNodePropertyStep.class);
        assertThat(step.procName()).isEqualTo(REMOTE_PROC_NAME);
        assertThat(step.mutateNodeProperty()).isEqualTo("embedding");
        assertThat(step.config()).containsEntry("mutateProperty", "embedding");
        assertThat(executor.validatedProcName).isEqualTo(REMOTE_PROC_NAME);
        assertThat(executor.validatedConfig).containsEntry("modelName", "myModel");
    }

    @Test
    void shouldNormalizeProcedureNameBeforeHandling() {
        var executor = FakeRemoteExecutor.handling(REMOTE_PROC_NAME);

        var step = NodePropertyStepFactory.createNodePropertyStep(
            "gds.remote",
            Map.of("mutateProperty", "embedding"),
            executor
        );

        assertThat(step.procName()).isEqualTo(REMOTE_PROC_NAME);
    }

    @Test
    void shouldStripContextConfigKeysFromRemoteStepConfig() {
        var executor = FakeRemoteExecutor.handling(REMOTE_PROC_NAME);
        var config = Map.<String, Object>of(
            "mutateProperty", "embedding",
            CONTEXT_NODE_LABELS, List.of("A"),
            CONTEXT_RELATIONSHIP_TYPES, List.of("REL")
        );

        var step = NodePropertyStepFactory.createNodePropertyStep("remote", config, executor);

        assertThat(step.config()).doesNotContainKeys(CONTEXT_NODE_LABELS, CONTEXT_RELATIONSHIP_TYPES);
        assertThat(step.contextNodeLabels()).containsExactly("A");
        assertThat(step.contextRelationshipTypes()).containsExactly("REL");
    }

    @Test
    void shouldPropagateRemoteValidationFailure() {
        var executor = FakeRemoteExecutor.handling(REMOTE_PROC_NAME);
        executor.validationFailure = new IllegalArgumentException("missing mutateProperty");

        assertThatThrownBy(() -> NodePropertyStepFactory.createNodePropertyStep(
            "remote",
            Map.of(),
            executor
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("missing mutateProperty");
    }

    @Test
    void shouldNotUseRemoteBranchWithoutExecutor() {
        assertThatThrownBy(() -> NodePropertyStepFactory.createNodePropertyStep(
            "remote",
            Map.of("mutateProperty", "embedding")
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not find a procedure called " + REMOTE_PROC_NAME);
    }

    @Test
    void shouldNotUseRemoteBranchWhenExecutorDoesNotHandleProcedure() {
        var executor = FakeRemoteExecutor.handling("gds.somethingElse.mutate");

        assertThatThrownBy(() -> NodePropertyStepFactory.createNodePropertyStep(
            "remote",
            Map.of("mutateProperty", "embedding"),
            executor
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Could not find a procedure called " + REMOTE_PROC_NAME);
    }

    private static final class FakeRemoteExecutor implements RemoteNodePropertyStepExecutor {
        private final Set<String> handledProcedures;
        private RuntimeException validationFailure;
        private String validatedProcName;
        private Map<String, Object> validatedConfig;

        private FakeRemoteExecutor(Set<String> handledProcedures) {
            this.handledProcedures = handledProcedures;
        }

        static FakeRemoteExecutor handling(String... procNames) {
            return new FakeRemoteExecutor(Set.of(procNames));
        }

        @Override
        public boolean handles(String procName) {
            return handledProcedures.contains(procName);
        }

        @Override
        public void validate(String procName, Map<String, Object> configuration) {
            this.validatedProcName = procName;
            this.validatedConfig = configuration;
            if (validationFailure != null) {
                throw validationFailure;
            }
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
