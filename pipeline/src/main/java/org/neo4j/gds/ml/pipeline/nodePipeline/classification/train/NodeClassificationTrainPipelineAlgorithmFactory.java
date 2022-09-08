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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import static org.neo4j.gds.ml.pipeline.PipelineCompanion.validateMainMetric;

public class NodeClassificationTrainPipelineAlgorithmFactory extends
    GraphStoreAlgorithmFactory<
        NodeClassificationTrainAlgorithm,
        NodeClassificationPipelineTrainConfig
    > {

    private final ExecutionContext executionContext;

    public NodeClassificationTrainPipelineAlgorithmFactory(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public NodeClassificationTrainAlgorithm build(
        GraphStore graphStore,
        NodeClassificationPipelineTrainConfig configuration,
        ProgressTracker progressTracker
    ) {
        var pipeline = PipelineCatalog.getTyped(
            configuration.username(),
            configuration.pipeline(),
            NodeClassificationTrainingPipeline.class
        );

        return build(graphStore, configuration, pipeline, progressTracker);
    }

    public NodeClassificationTrainAlgorithm build(
        GraphStore graphStore,
        NodeClassificationPipelineTrainConfig configuration,
        NodeClassificationTrainingPipeline pipeline,
        ProgressTracker progressTracker
    ) {
        validateMainMetric(pipeline, configuration.metrics().get(0).toString());

        var nodeFeatureProducer = NodeFeatureProducer
            .create(graphStore, configuration, executionContext, progressTracker);

        nodeFeatureProducer.validateNodePropertyStepsContextConfigs(pipeline.nodePropertySteps());

        return new NodeClassificationTrainAlgorithm(
            NodeClassificationTrain.create(
                graphStore,
                pipeline,
                configuration,
                nodeFeatureProducer,
                progressTracker
            ),
            pipeline,
            graphStore,
            configuration,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(NodeClassificationPipelineTrainConfig configuration) {
        var pipeline = PipelineCatalog.getTyped(
            configuration.username(),
            configuration.pipeline(),
            NodeClassificationTrainingPipeline.class
        );

        return MemoryEstimations.builder(NodeClassificationTrain.class.getSimpleName())
            .add(NodeClassificationTrain.estimate(
                pipeline,
                configuration,
                executionContext.modelCatalog()
            ))
            .build();
    }

    @Override
    public String taskName() {
        return "Node Classification Train Pipeline";
    }

    @Override
    public Task progressTask(GraphStore graphStore, NodeClassificationPipelineTrainConfig config) {

        var pipeline = PipelineCatalog.getTyped(
            config.username(),
            config.pipeline(),
            NodeClassificationTrainingPipeline.class
        );
        return progressTask(graphStore, pipeline);
    }

    public static Task progressTask(GraphStore graphStore, NodeClassificationTrainingPipeline pipeline) {
        return NodeClassificationTrain.progressTask(pipeline, graphStore.nodeCount());
    }
}
