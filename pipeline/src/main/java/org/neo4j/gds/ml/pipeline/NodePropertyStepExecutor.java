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
import org.neo4j.gds.config.ElementTypeValidator;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodePropertyStepExecutor<PIPELINE_CONFIG extends AlgoBaseConfig & GraphNameConfig> {

    private final ExecutionContext executionContext;
    private final PIPELINE_CONFIG config;
    private final GraphStore graphStore;
    private final Collection<NodeLabel> nodeLabels;
    private final Collection<RelationshipType> relTypes;
    private final ProgressTracker progressTracker;

    NodePropertyStepExecutor(
        ExecutionContext executionContext,
        PIPELINE_CONFIG config,
        GraphStore graphStore,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        ProgressTracker progressTracker
    ) {
        this.executionContext = executionContext;
        this.config = config;
        this.graphStore = graphStore;
        this.nodeLabels = nodeLabels;
        this.relTypes = relationshipTypes;
        this.progressTracker = progressTracker;
    }

    public static MemoryEstimation estimateNodePropertySteps(
        ModelCatalog modelCatalog,
        String username,
        List<ExecutableNodePropertyStep> nodePropertySteps,
        List<String> nodeLabels,
        List<String> relationshipTypes
    ) {
        var nodePropertyStepEstimations = nodePropertySteps
            .stream()
            .map(step -> step.estimate(modelCatalog, username, nodeLabels, relationshipTypes))
            .collect(Collectors.toList());

        // NOTE: This has the drawback, that we disregard the sizes of the mutate-properties, but it's a better approximation than adding all together.
        // Also, theoretically we clean the feature dataset after the node property steps have run, but we never account for this
        // in the memory estimation.
        return MemoryEstimations.maxEstimation("NodeProperty Steps", nodePropertyStepEstimations);
    }

    public static Task tasks(List<ExecutableNodePropertyStep> nodePropertySteps, long featureInputSize) {
        long volumeEstimation = 10 * featureInputSize;
        return Tasks.task(
            "Execute node property steps",
            nodePropertySteps.stream()
                .map(ExecutableNodePropertyStep::rootTaskName)
                .map(taskName -> Tasks.leaf(taskName, volumeEstimation))
                .collect(Collectors.toList())
        );
    }

    public void validNodePropertyStepsContextConfigs(List<ExecutableNodePropertyStep> steps) {
        for (ExecutableNodePropertyStep step : steps) {
            ElementTypeValidator.validate(
                graphStore,
                ElementTypeValidator.resolve(graphStore, step.contextNodeLabels()),
                formatWithLocale("contextNodeLabels for step `%s`", step.procName()));

            ElementTypeValidator.validateTypes(
                graphStore,
                ElementTypeValidator.resolveTypes(graphStore, step.contextRelationshipTypes()),
                formatWithLocale("contextRelationshipTypes for step `%s`", step.procName()));
        }
    }

    public void executeNodePropertySteps(List<ExecutableNodePropertyStep> steps) {
        progressTracker.beginSubTask("Execute node property steps");
        for (ExecutableNodePropertyStep step : steps) {
            progressTracker.beginSubTask();
            var featureInputNodeLabels = step.featureInputNodeLabels(graphStore, nodeLabels);
            var featureInputRelationshipTypes = step.featureInputRelationshipTypes(graphStore, relTypes);
            step.execute(executionContext, config.graphName(), featureInputNodeLabels, featureInputRelationshipTypes);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask("Execute node property steps");
    }

    public void cleanupIntermediateProperties(List<ExecutableNodePropertyStep> steps) {
        steps.stream().map(ExecutableNodePropertyStep::mutateNodeProperty).forEach(graphStore::removeNodeProperty);
    }

    public static <CONFIG extends AlgoBaseConfig & GraphNameConfig> NodePropertyStepExecutor<CONFIG> of(
        ExecutionContext executionContext,
        GraphStore graphStore,
        CONFIG config,
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        ProgressTracker progressTracker
    ) {
        return new NodePropertyStepExecutor<>(
            executionContext,
            config,
            graphStore,
            nodeLabels,
            relationshipTypes,
            progressTracker
        );
    }
}
