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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GraphStoreValidation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;

public abstract class PipelineExecutor<
    PIPELINE_CONFIG extends AlgoBaseConfig,
    PIPELINE extends Pipeline<?>,
    RESULT
    > extends Algorithm<RESULT> {

    public enum DatasetSplits {
        TRAIN,
        TEST,
        TEST_COMPLEMENT,
        FEATURE_INPUT
    }

    protected final PIPELINE pipeline;
    protected final PIPELINE_CONFIG config;
    protected final ExecutionContext executionContext;
    protected final GraphStore graphStore;
    protected final GraphSchema schemaBeforeSteps;
    protected final String graphName;

    protected PipelineExecutor(
        PIPELINE pipeline,
        PIPELINE_CONFIG config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        String graphName,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipeline = pipeline;
        this.config = config;
        this.executionContext = executionContext;
        this.graphStore = graphStore;
        this.graphName = graphName;
        this.schemaBeforeSteps = graphStore
            .schema()
            .filterNodeLabels(Set.copyOf(config.nodeLabelIdentifiers(graphStore)))
            .filterRelationshipTypes(Set.copyOf(config.internalRelationshipTypes(graphStore)));
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

    protected static Task nodePropertyStepTasks(List<ExecutableNodePropertyStep> nodePropertySteps, long featureInputSize) {
        long volumeEstimation = 10 * featureInputSize;
        return Tasks.task(
            "Execute node property steps",
            nodePropertySteps.stream()
                .map(ExecutableNodePropertyStep::rootTaskName)
                .map(taskName -> Tasks.leaf(taskName, volumeEstimation))
                .collect(Collectors.toList())
        );
    }

    public static void validateTrainingParameterSpace(TrainingPipeline pipeline) {
        if (pipeline.numberOfModelSelectionTrials() == 0) {
            throw new IllegalArgumentException("Need at least one model candidate for training.");
        }
    }

    public abstract Map<DatasetSplits, GraphFilter> splitDataset();

    protected abstract RESULT execute(Map<DatasetSplits, GraphFilter> dataSplits);

    @Override
    public RESULT compute() {
        progressTracker.beginSubTask();

        pipeline.validateBeforeExecution(graphStore, config);

        var dataSplits = splitDataset();
        try {
            progressTracker.beginSubTask("Execute node property steps");
            // we are not validating the size of the feature-input graph as not every nodePropertyStep needs relationships
            executeNodePropertySteps(dataSplits.get(DatasetSplits.FEATURE_INPUT));
            progressTracker.endSubTask("Execute node property steps");

            validate(graphStore, config);

            var result = execute(dataSplits);
            progressTracker.endSubTask();
            return result;
        } finally {
            cleanUpGraphStore(dataSplits);
        }
    }

    protected void validate(GraphStore graphStore, PIPELINE_CONFIG config) {
        this.pipeline.validateFeatureProperties(graphStore, config);
        GraphStoreValidation.validate(graphStore, config);
    }

    @Override
    public void release() {

    }

    private void executeNodePropertySteps(GraphFilter graphFilter) {
        for (ExecutableNodePropertyStep step : pipeline.nodePropertySteps()) {
            progressTracker.beginSubTask();
            step.execute(executionContext, graphName, graphFilter.nodeLabels(), graphFilter.relationshipTypes());
            progressTracker.endSubTask();
        }
    }

    protected void cleanUpGraphStore(Map<DatasetSplits, GraphFilter> datasets) {
        removeNodeProperties(graphStore, config.nodeLabelIdentifiers(graphStore));
    }

    private void removeNodeProperties(GraphStore graphstore, Iterable<NodeLabel> nodeLabels) {
        pipeline.nodePropertySteps().forEach(step -> {
            var intermediateProperty = step.config().get(MUTATE_PROPERTY_KEY);
            if (intermediateProperty instanceof String) {
                graphstore.removeNodeProperty(((String) intermediateProperty));
            }
        });
    }

    @ValueClass
    public interface GraphFilter {
        Collection<NodeLabel> nodeLabels();

        Collection<RelationshipType> relationshipTypes();
    }
}
