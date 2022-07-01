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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.nodeproperties.LongTestPropertyValues;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

public class ExecutableNodePropertyStepTestUtil {
    static class AddBogusNodePropertyStep implements ExecutableNodePropertyStep {
        static final String PROPERTY = "someBogusProperty";
        private final GraphStore graphStore;

        AddBogusNodePropertyStep(GraphStore graphStore) {
            this.graphStore = graphStore;
        }

        @Override
        public String procName() {
            return "AddBogusNodePropertyStep";
        }

        @Override
        public MemoryEstimation estimate(ModelCatalog modelCatalog, List<String> nodeLabels, List<String> relTypes) {
            throw new MemoryEstimationNotImplementedException();
        }

        @Override
        public void execute(
            ExecutionContext executionContext,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes
        ) {
            graphStore.addNodeProperty(
                graphStore.nodeLabels(),
                PROPERTY,
                new LongTestPropertyValues(nodeId -> nodeId)
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

    static class FailingNodePropertyStep implements ExecutableNodePropertyStep {
        static final String PROPERTY = "failingStepProperty";
        @Override
        public String procName() {
            return "FailingNodePropertyStep";
        }

        @Override
        public MemoryEstimation estimate(ModelCatalog modelCatalog, List<String> nodeLabels, List<String> relTypes) {
            throw new MemoryEstimationNotImplementedException();
        }

        @Override
        public void execute(
            ExecutionContext executionContext,
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

    static final class PipelineExecutionTestExecuteNodeStepFailure extends RuntimeException {}
}
