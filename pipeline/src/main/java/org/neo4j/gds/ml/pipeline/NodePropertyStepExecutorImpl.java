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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Collection;

public class NodePropertyStepExecutorImpl<PIPELINE_CONFIG extends AlgoBaseConfig & GraphNameConfig> implements NodePropertyStepExecutor {
    private final ExecutionContext executionContext;
    private final PIPELINE_CONFIG config;
    private final GraphStore graphStore;
    private final Collection<NodeLabel> nodeLabels;
    private final Collection<RelationshipType> relTypes;
    private final ProgressTracker progressTracker;

    NodePropertyStepExecutorImpl(
        ExecutionContext executionContext,
        PIPELINE_CONFIG config,
        GraphStore graphStore,
        Collection<RelationshipType> relationshipTypes,
        ProgressTracker progressTracker
    ) {
        this.executionContext = executionContext;
        this.config = config;
        this.graphStore = graphStore;
        this.nodeLabels = config.nodeLabelIdentifiers(graphStore);
        this.relTypes = relationshipTypes;
        this.progressTracker = progressTracker;
    }

    public void executeNodePropertySteps(Pipeline<?> pipeline) {
        progressTracker.beginSubTask("Execute node property steps");
        for (ExecutableNodePropertyStep step : pipeline.nodePropertySteps()) {
            progressTracker.beginSubTask();
            step.execute(executionContext, config.graphName(), nodeLabels, relTypes);
            progressTracker.endSubTask();
        }
        pipeline.validateFeatureProperties(graphStore, config);
        progressTracker.endSubTask("Execute node property steps");
    }
}
