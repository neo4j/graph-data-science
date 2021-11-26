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
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.nodeproperties.LongTestProperties;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

@GdlExtension
class PipelineExecutorTest {

    private static final NodeLabel NODE_LABEL_N = NodeLabel.of("N");

    @GdlGraph
    static String GDL = "(n:N)";

    @Inject
    private GraphStore graphStore;

    @Test
    void shouldCleanGraphStoreOnFailureWhenExecuting() {
        var pipelineExecutor = new FailingPipelineExecutor(
            new BogusNodePropertyPipeline(),
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(PipelineExecutionTestFailure.class);
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, AddBogusNodePropertyStep.PROPERTY)).isFalse();
    }

    @Test
    void shouldCleanGraphStoreWhenNodePropertyStepIsFailing() {
        var pipelineExecutor = new SucceedingPipelineExecutor(
            new FailingNodePropertyPipeline(),
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(PipelineExecutionTestExecuteNodeStepFailure.class);
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, AddBogusNodePropertyStep.PROPERTY)).isFalse();
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, FailingNodePropertyStep.PROPERTY)).isFalse();
    }

    @Test
    void shouldHaveCorrectProgressLoggingOnFailure() {
        var log = new TestLog();
        var pipelineExecutor = new FailingPipelineExecutor(
            new BogusNodePropertyPipeline(),
            new PipelineExecutorTestConfig(),
            new TestProgressTracker(taskTree(), log, 1, EmptyTaskRegistryFactory.INSTANCE)
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(PipelineExecutionTestFailure.class);
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "FailingPipelineExecutor :: Start",
                "FailingPipelineExecutor :: execute node property steps :: Start",
                "FailingPipelineExecutor :: execute node property steps :: step 1 of 1 :: Start",
                "FailingPipelineExecutor :: execute node property steps :: step 1 of 1 100%",
                "FailingPipelineExecutor :: execute node property steps :: step 1 of 1 :: Finished",
                "FailingPipelineExecutor :: execute node property steps :: Finished"
                // TODO: Figure out why is this not logged!?
                // "FailingPipelineExecutor :: Failed"
            );
    }

    private Task taskTree() {
        return Tasks.task(
            "FailingPipelineExecutor",
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                1
            )
        );
    }

    private static final class PipelineExecutionTestFailure extends RuntimeException {}
    private static final class PipelineExecutionTestExecuteNodeStepFailure extends RuntimeException {}

    private static class PipelineExecutorTestConfig implements AlgoBaseConfig {
        @Override
        public Optional<String> graphName() {
            return Optional.empty();
        }

        @Override
        public Collection<NodeLabel> nodeLabelIdentifiers(GraphStore graphStore) {
            return List.of(NODE_LABEL_N);
        }

        @Override
        public Optional<GraphCreateConfig> implicitCreateConfig() {
            return Optional.empty();
        }
    }

    private class SucceedingPipelineExecutor extends PipelineExecutor<AlgoBaseConfig, Pipeline<FeatureStep, ToMapConvertible>, String, SucceedingPipelineExecutor> {
        SucceedingPipelineExecutor(
            Pipeline<FeatureStep, ToMapConvertible> pipelineStub,
            AlgoBaseConfig config,
            ProgressTracker progressTracker
        ) {
            super(
                pipelineStub,
                config,
                null,
                PipelineExecutorTest.this.graphStore,
                "graph",
                progressTracker
            );
        }

        @Override
        public Map<DatasetSplits, GraphFilter> splitDataset() {
            return Map.of(DatasetSplits.FEATURE_INPUT, ImmutableGraphFilter.of(List.of(NODE_LABEL_N), List.of()));
        }

        @Override
        protected String execute(Map<DatasetSplits, GraphFilter> dataSplits) {
            return "I am not failing";
        }

        @Override
        public SucceedingPipelineExecutor me() {
            return this;
        }
    }

    private class FailingPipelineExecutor extends SucceedingPipelineExecutor {
        FailingPipelineExecutor(
            Pipeline<FeatureStep, ToMapConvertible> pipelineStub,
            AlgoBaseConfig config,
            ProgressTracker progressTracker
        ) {
            super(
                pipelineStub,
                config,
                progressTracker
            );
        }

        @Override
        protected String execute(Map<DatasetSplits, GraphFilter> dataSplits) {
            throw new PipelineExecutionTestFailure();
        }
    }

    private class AddBogusNodePropertyStep implements ExecutableNodePropertyStep {
        static final String PROPERTY = "someBogusProperty";

        @Override
        public String procName() {
            return "AddBogusNodePropertyStep";
        }

        @Override
        public Method procMethod() {
            return null;
        }

        @Override
        public void execute(
            BaseProc caller,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes
        ) {
            graphStore.addNodeProperty(
                NODE_LABEL_N,
                PROPERTY,
                new LongTestProperties(nodeId -> nodeId)
            );
        }

        @Override
        public Map<String, Object> config() {
            return Map.of(MUTATE_PROPERTY_KEY, PROPERTY);
        }

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }
    }

    private static class FailingNodePropertyStep implements ExecutableNodePropertyStep {
        static final String PROPERTY = "failingStepProperty";
        @Override
        public String procName() {
            return "FailingNodePropertyStep";
        }

        @Override
        public Method procMethod() {
            return null;
        }

        @Override
        public void execute(
            BaseProc caller,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes
        ) {
            throw new PipelineExecutionTestExecuteNodeStepFailure();
        }

        @Override
        public Map<String, Object> config() {
            return Map.of(MUTATE_PROPERTY_KEY, PROPERTY);
        }

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }
    }

    private class BogusNodePropertyPipeline extends Pipeline<FeatureStep, ToMapConvertible> {

        BogusNodePropertyPipeline() {super(List.of());}

        @Override
        public List<ExecutableNodePropertyStep> nodePropertySteps() {
            return List.of(new AddBogusNodePropertyStep());
        }

        @Override
        protected Map<String, Object> additionalEntries() {
            return Map.of();
        }

        @Override
        public void validateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void validate(GraphStore graphStore, AlgoBaseConfig config) {}
    }

    private class FailingNodePropertyPipeline extends Pipeline<FeatureStep, ToMapConvertible> {

        FailingNodePropertyPipeline() {super(List.of());}

        @Override
        public List<ExecutableNodePropertyStep> nodePropertySteps() {
            return List.of(new AddBogusNodePropertyStep(), new FailingNodePropertyStep());
        }

        @Override
        protected Map<String, Object> additionalEntries() {
            return Map.of();
        }

        @Override
        public void validateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void validate(GraphStore graphStore, AlgoBaseConfig config) {}
    }
}
