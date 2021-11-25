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
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.ToMapConvertible;
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
import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

@GdlExtension
class PipelineExecutorTest {

    private static final NodeLabel NODE_LABEL_N = NodeLabel.of("N");

    @GdlGraph
    static String GDL = "(n:N)";

    @Inject
    private GraphStore graphStore;

    @Test
    void shouldCleanGraphStoreOnFailure() {

        var pipelineStub = new BogusNodePropertyPipeline();

        var config = new AlgoBaseConfig() {
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
        };

        var pipelineExecutor = new FailingPipelineExecutor(pipelineStub, config);

        assertThatThrownBy(pipelineExecutor::compute)
            .isExactlyInstanceOf(PipelineExecutionTestFailure.class);

        assertThat(graphStore.hasNodeProperty(NODE_LABEL_N, AddBogusNodePropertyStep.PROPERTY))
            .isFalse();
    }

    private static final class PipelineExecutionTestFailure extends RuntimeException {}

    private class FailingPipelineExecutor extends PipelineExecutor<AlgoBaseConfig, Pipeline<FeatureStep, ToMapConvertible>, Object, FailingPipelineExecutor> {
        FailingPipelineExecutor(Pipeline<FeatureStep, ToMapConvertible> pipelineMock, AlgoBaseConfig config) {
            super(
                pipelineMock,
                config,
                null,
                PipelineExecutorTest.this.graphStore,
                "graph",
                TestProgressTracker.NULL_TRACKER
            );
        }

        @Override
        public Map<DatasetSplits, GraphFilter> splitDataset() {
            return Map.of(DatasetSplits.FEATURE_INPUT, ImmutableGraphFilter.of(List.of(NODE_LABEL_N), List.of()));
        }

        @Override
        protected Object execute(Map<DatasetSplits, GraphFilter> dataSplits) {
            throw new PipelineExecutionTestFailure();
        }

        @Override
        public FailingPipelineExecutor me() {
            return this;
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
}
