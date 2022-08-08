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

import org.immutables.value.Value;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphNameConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PipelineExecutor<
    PIPELINE_CONFIG extends AlgoBaseConfig & GraphNameConfig,
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

    protected PipelineExecutor(
        PIPELINE pipeline,
        PIPELINE_CONFIG config,
        ExecutionContext executionContext,
        GraphStore graphStore,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pipeline = pipeline;
        this.config = config;
        this.executionContext = executionContext;
        this.graphStore = graphStore;
        this.schemaBeforeSteps = graphStore
            .schema()
            .filterNodeLabels(Set.copyOf(config.nodeLabelIdentifiers(graphStore)))
            .filterRelationshipTypes(Set.copyOf(config.internalRelationshipTypes(graphStore)));
    }

    public abstract Map<DatasetSplits, GraphFilter> splitDataset();

    protected abstract RESULT execute(Map<DatasetSplits, GraphFilter> dataSplits);

    @Override
    public RESULT compute() {
        progressTracker.beginSubTask();

        //TODO Refactor actually splitting to after validateBeforeExecution, and only keep metadata generation here
        var dataSplits = splitDataset();
        var graphFilter = dataSplits.get(DatasetSplits.FEATURE_INPUT);

        pipeline.validateBeforeExecution(graphStore, graphFilter.nodeLabels());

        var nodePropertyStepExecutor = NodePropertyStepExecutor.of(
            executionContext,
            graphStore,
            config,
            graphFilter.nodeLabels(),
            graphFilter.relationshipTypes(),
            progressTracker
        );

        try {
            // we are not validating the size of the feature-input graph as not every nodePropertyStep needs relationships
            nodePropertyStepExecutor.executeNodePropertySteps(pipeline.nodePropertySteps());
            pipeline.validateFeatureProperties(graphStore, config.nodeLabelIdentifiers(graphStore));

            var result = execute(dataSplits);
            progressTracker.endSubTask();
            return result;
        } finally {
            nodePropertyStepExecutor.cleanupIntermediateProperties(pipeline.nodePropertySteps());
            additionalGraphStoreCleanup(dataSplits);
        }
    }

    @Override
    public void release() {

    }

    protected void additionalGraphStoreCleanup(Map<DatasetSplits, GraphFilter> datasets) {
    }

    @ValueClass
    public interface GraphFilter {
        Collection<NodeLabel> nodeLabels();

        @Value.Derived
        default Collection<RelationshipType> relationshipTypes() {
            return Stream.of(intermediateRelationshipTypes(), contextRelationshipTypes()).flatMap(Collection::stream).collect(
                Collectors.toSet());
        }

        @Value.Default
        default Collection<RelationshipType> intermediateRelationshipTypes() {
            return List.of();
        }

        @Value.Default
        default Collection<RelationshipType> contextRelationshipTypes() {
            return List.of();
        }
    }
}
