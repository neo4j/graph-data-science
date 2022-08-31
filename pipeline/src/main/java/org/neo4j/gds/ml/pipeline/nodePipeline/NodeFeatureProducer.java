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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.models.FeaturesFactory;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;

import java.util.Collection;

public final class NodeFeatureProducer<PIPELINE_CONFIG extends NodePropertyPipelineBaseTrainConfig> {

    private final NodePropertyStepExecutor<PIPELINE_CONFIG> stepExecutor;
    private final GraphStore graphStore;
    private final PIPELINE_CONFIG trainConfig;

    private NodeFeatureProducer(
        NodePropertyStepExecutor<PIPELINE_CONFIG> stepExecutor,
        GraphStore graphStore,
        PIPELINE_CONFIG trainConfig
    ) {
        this.stepExecutor = stepExecutor;
        this.graphStore = graphStore;
        this.trainConfig = trainConfig;
    }

    public static <PIPELINE_CONFIG extends NodePropertyPipelineBaseTrainConfig> NodeFeatureProducer<PIPELINE_CONFIG> create(
        GraphStore graphStore,
        PIPELINE_CONFIG config,
        ExecutionContext executionContext,
        ProgressTracker progressTracker
    ) {

        return new NodeFeatureProducer<>(
            NodePropertyStepExecutor.of(
                executionContext,
                graphStore,
                config,
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore),
                progressTracker
            ),
            graphStore,
            config
        );
    }

    public Features procedureFeatures(NodePropertyTrainingPipeline pipeline) {
        try {
            stepExecutor.executeNodePropertySteps(pipeline.nodePropertySteps());
            Collection<NodeLabel> targetNodeLabels = trainConfig.nodeLabelIdentifiers(graphStore);
            pipeline.validateFeatureProperties(graphStore, targetNodeLabels);

            // We create a filtered graph with only targetNodeLabels, that contains the newly created node properties from the steps
            var targetNodeLabelGraph = graphStore.getGraph(targetNodeLabels);
            if (pipeline.requireEagerFeatures()) {
                // Random forest uses feature vectors many times each.
                return FeaturesFactory.extractEagerFeatures(targetNodeLabelGraph, pipeline.featureProperties());
            } else {
                return FeaturesFactory.extractLazyFeatures(targetNodeLabelGraph, pipeline.featureProperties());
            }
        } finally {
            stepExecutor.cleanupIntermediateProperties(pipeline.nodePropertySteps());
        }
    }
}
