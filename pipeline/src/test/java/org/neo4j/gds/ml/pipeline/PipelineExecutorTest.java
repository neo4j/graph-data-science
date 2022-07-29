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
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@GdlExtension
class PipelineExecutorTest {

    private static final NodeLabel NODE_LABEL_N = NodeLabel.of("N");

    @GdlGraph
    static String GDL = "(n:N)";

    @Inject
    private GraphStore graphStore;

    @Test
    void shouldCleanGraphStoreWhenComputationIsComplete() {
        var pipelineExecutor = new SucceedingPipelineExecutor(
            new BogusNodePropertyPipeline(),
            graphStore,
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatNoException().isThrownBy(pipelineExecutor::compute);
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, "someBogusProperty")).isFalse();
    }

    @Test
    void shouldCleanGraphStoreOnFailureWhenExecuting() {
        var pipelineExecutor = new FailingPipelineExecutor(
            new BogusNodePropertyPipeline(),
            graphStore,
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(PipelineExecutionTestFailure.class);
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, "someBogusProperty")).isFalse();
    }

    @Test
    void shouldHaveCorrectProgressLoggingOnSuccessfulComputation() {
        var log = Neo4jProxy.testLog();
        var pipeline = new BogusNodePropertyPipeline();
        var pipelineExecutor = new SucceedingPipelineExecutor(
            pipeline,
            graphStore,
            new PipelineExecutorTestConfig(),
            new TestProgressTracker(taskTree(pipeline), log, 1, EmptyTaskRegistryFactory.INSTANCE)
        );

        assertThatNoException().isThrownBy(pipelineExecutor::compute);
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "FailingPipelineExecutor :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep 100%",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep :: Finished",
                "FailingPipelineExecutor :: Execute node property steps :: Finished",
                "FailingPipelineExecutor :: Finished"
            );
    }

    @Test
    void shouldCleanGraphStoreWhenNodePropertyStepIsFailing() {
        var pipelineExecutor = new SucceedingPipelineExecutor(
            new FailingNodePropertyPipeline(),
            graphStore,
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(ExecutableNodePropertyStepTestUtil.PipelineExecutionTestExecuteNodeStepFailure.class);
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, "someBogusProperty")).isFalse();
        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, ExecutableNodePropertyStepTestUtil.FailingNodePropertyStep.PROPERTY)).isFalse();
    }

    @Test
    void shouldHaveCorrectProgressLoggingOnFailure() {
        var log = Neo4jProxy.testLog();
        var pipeline = new BogusNodePropertyPipeline();
        var pipelineExecutor = new FailingPipelineExecutor(
            pipeline,
            graphStore,
            new PipelineExecutorTestConfig(),
            new TestProgressTracker(taskTree(pipeline), log, 1, EmptyTaskRegistryFactory.INSTANCE)
        );

        assertThatThrownBy(pipelineExecutor::compute).isExactlyInstanceOf(PipelineExecutionTestFailure.class);
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "FailingPipelineExecutor :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep :: Start",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep 100%",
                "FailingPipelineExecutor :: Execute node property steps :: AddBogusNodePropertyStep :: Finished",
                "FailingPipelineExecutor :: Execute node property steps :: Finished"
            );
    }

    @Test
    void failOnInvalidGraphBeforeExecution() {
        var invalidGraphStore = GdlFactory.of("(), ()").build();
        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(),
            Stream.of(new L2FeatureStep(List.of("a")))
        );

        var executor = new FailingPipelineExecutor(
            pipeline,
            invalidGraphStore,
            new PipelineExecutorTestConfig(),
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(executor::compute)
            .hasMessageContaining("Node properties [a] defined in the feature steps do not exist in the graph or part of the pipeline");
    }


    private Task taskTree(TrainingPipeline<?> pipeline) {
        return Tasks.task(
            "FailingPipelineExecutor",
            NodePropertyStepExecutor.tasks(pipeline.nodePropertySteps(), 10)
        );
    }

    private static final class PipelineExecutionTestFailure extends RuntimeException {}

    private static class PipelineExecutorTestConfig implements AlgoBaseConfig, GraphNameConfig {
        @Override
        public String graphName() {
            return "test";
        }

        @Override
        public Collection<NodeLabel> nodeLabelIdentifiers(GraphStore graphStore) {
            return List.of(NODE_LABEL_N);
        }
    }

    private static class SucceedingPipelineExecutor<CONFIG extends AlgoBaseConfig & GraphNameConfig>
        extends PipelineExecutor<CONFIG, Pipeline<? extends FeatureStep>, String> {
        SucceedingPipelineExecutor(
            Pipeline<? extends FeatureStep> pipelineStub,
            GraphStore graphStore,
            CONFIG config,
            ProgressTracker progressTracker
        ) {
            super(
                pipelineStub,
                config,
                ExecutionContext.EMPTY,
                graphStore,
                progressTracker
            );
        }

        @Override
        public Map<DatasetSplits, GraphFilter> splitDataset() {
            return Map.of(DatasetSplits.FEATURE_INPUT, ImmutableGraphFilter.builder().nodeLabels(List.of(NODE_LABEL_N)).build());
        }

        @Override
        protected String execute(Map<DatasetSplits, GraphFilter> dataSplits) {
            return "I am not failing";
        }

    }

    private static class FailingPipelineExecutor<CONFIG extends AlgoBaseConfig & GraphNameConfig> extends PipelineExecutorTest.SucceedingPipelineExecutor<CONFIG> {
        FailingPipelineExecutor(
            Pipeline<? extends FeatureStep> pipelineStub,
            GraphStore graphStore,
            CONFIG config,
            ProgressTracker progressTracker
        ) {
            super(
                pipelineStub,
                graphStore,
                config,
                progressTracker
            );
        }

        @Override
        protected String execute(Map<DatasetSplits, GraphFilter> dataSplits) {
            throw new PipelineExecutorTest.PipelineExecutionTestFailure();
        }
    }

    private class BogusNodePropertyPipeline extends TrainingPipeline<FeatureStep> {

        BogusNodePropertyPipeline() {super(TrainingType.REGRESSION);}

        @Override
        public List<ExecutableNodePropertyStep> nodePropertySteps() {
            return List.of(new ExecutableNodePropertyStepTestUtil.NodeIdPropertyStep(graphStore, "someBogusProperty"));
        }

        @Override
        public String type() {
            return "bogus pipeline";
        }

        @Override
        protected Map<String, List<Map<String, Object>>> featurePipelineDescription() {
            return Map.of();
        }

        @Override
        protected Map<String, Object> additionalEntries() {
            return Map.of();
        }

        @Override
        public void validateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void specificValidateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void validateFeatureProperties(GraphStore graphStore, AlgoBaseConfig config) {}
    }

    private class FailingNodePropertyPipeline extends TrainingPipeline<FeatureStep> {

        FailingNodePropertyPipeline() {super(TrainingType.CLASSIFICATION);}

        @Override
        public List<ExecutableNodePropertyStep> nodePropertySteps() {
            return List.of(new ExecutableNodePropertyStepTestUtil.NodeIdPropertyStep(graphStore, "someBogusProperty"), new ExecutableNodePropertyStepTestUtil.FailingNodePropertyStep());
        }

        @Override
        public String type() {
            return "failing pipeline";
        }

        @Override
        protected Map<String, List<Map<String, Object>>> featurePipelineDescription() {
            return Map.of();
        }

        @Override
        protected Map<String, Object> additionalEntries() {
            return Map.of();
        }

        @Override
        public void validateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void specificValidateBeforeExecution(GraphStore graphStore, AlgoBaseConfig config) {}

        @Override
        public void validateFeatureProperties(GraphStore graphStore, AlgoBaseConfig config) {}
    }
}
