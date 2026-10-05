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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_NODE_LABELS;
import static org.neo4j.gds.ml.pipeline.NodePropertyStepContextConfig.CONTEXT_RELATIONSHIP_TYPES;

class RemoteNodePropertyStepTest {

    private static final String PROC_NAME = "gds.remote.mutate";

    @Test
    void shouldDelegateToRemoteExecutorWithPipelineScopingAndWithoutInjectingConcurrency() {
        var executor = new RecordingRemoteExecutor();
        var step = new RemoteNodePropertyStep(
            PROC_NAME,
            Map.of("modelName", "myModel", "mutateProperty", "embedding"),
            List.of(),
            List.of(),
            executor
        );

        step.execute(
            ExecutionContext.EMPTY,
            "graph",
            List.of(NodeLabel.of("A"), NodeLabel.of("B")),
            List.of(RelationshipType.of("REL")),
            new Concurrency(4),
            null
        );

        assertThat(executor.procName).isEqualTo(PROC_NAME);
        assertThat(executor.graphName).isEqualTo("graph");
        assertThat(executor.configuration)
            .containsEntry("modelName", "myModel")
            .containsEntry("mutateProperty", "embedding")
            .doesNotContainKey("concurrency");
        assertThat(executor.nodeLabels).containsExactlyInAnyOrder("A", "B");
        assertThat(executor.relationshipTypes).containsExactly("REL");
        assertThat(executor.terminationMonitor).isSameAs(ExecutionContext.EMPTY.terminationMonitor());
    }

    @Test
    void shouldNotMutateTheStepConfigurationOnExecute() {
        var executor = new RecordingRemoteExecutor();
        var stepConfig = Map.<String, Object>of("mutateProperty", "embedding");
        var step = new RemoteNodePropertyStep(PROC_NAME, stepConfig, List.of(), List.of(), executor);

        step.execute(
            ExecutionContext.EMPTY,
            "graph",
            List.of(NodeLabel.of("A")),
            List.of(RelationshipType.of("REL")),
            new Concurrency(1),
            null
        );

        assertThat(step.config()).isEqualTo(stepConfig);
    }

    @Test
    void shouldExposeTheMutateProperty() {
        var step = new RemoteNodePropertyStep(
            PROC_NAME,
            Map.of("mutateProperty", "embedding"),
            List.of(),
            List.of(),
            new RecordingRemoteExecutor()
        );

        assertThat(step.mutateNodeProperty()).isEqualTo("embedding");
        assertThat(step.procName()).isEqualTo(PROC_NAME);
    }

    @Test
    void shouldReturnZeroEstimationBecauseComputeHappensRemotely() {
        var step = new RemoteNodePropertyStep(
            PROC_NAME,
            Map.of("mutateProperty", "embedding"),
            List.of(),
            List.of(),
            new RecordingRemoteExecutor()
        );

        var estimation = step.estimate(null, null, "user", List.of(), List.of(), null);

        assertThat(estimation.description()).contains(PROC_NAME);
    }

    @Test
    void shouldIncludeContextConfigInToMap() {
        var step = new RemoteNodePropertyStep(
            PROC_NAME,
            Map.of("mutateProperty", "embedding"),
            List.of("A"),
            List.of("REL"),
            new RecordingRemoteExecutor()
        );

        assertThat(step.toMap()).containsEntry("name", PROC_NAME);
        @SuppressWarnings("unchecked")
        var config = (Map<String, Object>) step.toMap().get("config");
        assertThat(config)
            .containsEntry("mutateProperty", "embedding")
            .containsEntry(CONTEXT_NODE_LABELS, List.of("A"))
            .containsEntry(CONTEXT_RELATIONSHIP_TYPES, List.of("REL"));
    }

    private static final class RecordingRemoteExecutor implements RemoteNodePropertyStepExecutor {
        private String procName;
        private String graphName;
        private Map<String, Object> configuration;
        private List<String> nodeLabels;
        private List<String> relationshipTypes;
        private TerminationMonitor terminationMonitor;

        @Override
        public boolean handles(String procName) {
            return PROC_NAME.equals(procName);
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
            this.procName = procName;
            this.graphName = graphName;
            this.configuration = configuration;
            this.nodeLabels = nodeLabels;
            this.relationshipTypes = relationshipTypes;
            this.terminationMonitor = terminationMonitor;
        }
    }
}
