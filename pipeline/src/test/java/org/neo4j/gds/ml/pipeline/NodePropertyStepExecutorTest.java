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
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.assertj.Extractors;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStepTestUtil.NodeIdPropertyStep;

class NodePropertyStepExecutorTest {

    @Test
    void executeSeveralSteps() {
        var graphStore = GdlFactory.of("(a {age: 12})-->(b {age: 42})").build();

        var executor = new NodePropertyStepExecutor<>(
            ExecutionContext.EMPTY,
            new NodePropertyStepExecutorTestConfig(),
            graphStore,
            graphStore.relationshipTypes(),
            ProgressTracker.NULL_TRACKER
        );

        assertThat(graphStore.nodePropertyKeys()).containsExactly("age");

        List<ExecutableNodePropertyStep> steps = List.of(
            new NodeIdPropertyStep(graphStore, "prop1"),
            new NodeIdPropertyStep(graphStore, "prop2")
        );

        executor.executeNodePropertySteps(steps);

        assertThat(graphStore.nodePropertyKeys()).containsExactly("age", "prop1", "prop2");

        executor.cleanupIntermediateProperties(steps);

        assertThat(graphStore.nodePropertyKeys()).containsExactly("age");
    }

    @Test
    void progressLogging() {
        var graphStore = GdlFactory.of("(a {age: 12})-->(b {age: 42})").build();

        var steps = List.<ExecutableNodePropertyStep>of(
            new NodeIdPropertyStep(graphStore, "one proc", "prop1"),
            new NodeIdPropertyStep(graphStore, "another proc", "prop2")
        );

        var progressTracker = new InspectableTestProgressTracker(NodePropertyStepExecutor.tasks(steps, graphStore.nodeCount()), "user", JobId.parse("42"));

        new NodePropertyStepExecutor<>(
            ExecutionContext.EMPTY,
            new NodePropertyStepExecutorTestConfig(),
            graphStore,
            graphStore.relationshipTypes(),
            progressTracker
        ).executeNodePropertySteps(steps);

        assertThat(progressTracker.log().getMessages(TestLog.INFO))
            .extracting(Extractors.removingThreadId())
            .containsExactly(
                "Execute node property steps :: Start",
                "Execute node property steps :: one proc :: Start",
                "Execute node property steps :: one proc 100%",
                "Execute node property steps :: one proc :: Finished",
                "Execute node property steps :: another proc :: Start",
                "Execute node property steps :: another proc 100%",
                "Execute node property steps :: another proc :: Finished",
                "Execute node property steps :: Finished"
        );
    }

    @Test
    void memoryEstimation() {
        MemoryEstimation firstEstimation = MemoryEstimations.of("chocolate", MemoryRange.of(42));
        MemoryEstimation secondEstimation = MemoryEstimations.of("chocolate", MemoryRange.of(1337));
        var steps = List.<ExecutableNodePropertyStep>of(
            new ExecutableNodePropertyStepTestUtil.TestNodePropertyStepWithFixedEstimation("SmallProc", firstEstimation),
            new ExecutableNodePropertyStepTestUtil.TestNodePropertyStepWithFixedEstimation("LargeProc", secondEstimation)
        );

        var dimensions = GraphDimensions.of(10);

        MemoryEstimation actualEstimation = NodePropertyStepExecutor.estimateNodePropertySteps(
            new OpenModelCatalog(),
            steps,
            List.of(),
            List.of()
        );

        assertThat(actualEstimation.estimate(dimensions, 4).memoryUsage()).isEqualTo(MemoryRange.of(1337));
    }

    private static class NodePropertyStepExecutorTestConfig implements AlgoBaseConfig, GraphNameConfig {
        @Override
        public String graphName() {
            return "test";
        }

        @Override
        public Optional<String> usernameOverride() {
            return Optional.empty();
        }
    }

}
