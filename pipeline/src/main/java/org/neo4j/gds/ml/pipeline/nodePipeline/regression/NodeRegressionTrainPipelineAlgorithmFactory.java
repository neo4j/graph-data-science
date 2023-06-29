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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;

import static org.neo4j.gds.ml.pipeline.PipelineCompanion.validateMainMetric;

public class NodeRegressionTrainPipelineAlgorithmFactory extends GraphStoreAlgorithmFactory<NodeRegressionTrainAlgorithm, NodeRegressionPipelineTrainConfig> {

    private final ExecutionContext executionContext;

    public NodeRegressionTrainPipelineAlgorithmFactory(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public NodeRegressionTrainAlgorithm build(
        GraphStore graphStore,
        NodeRegressionPipelineTrainConfig configuration,
        ProgressTracker progressTracker
    ) {
        var pipeline = PipelineCatalog.getTyped(
            configuration.username(),
            configuration.pipeline(),
            NodeRegressionTrainingPipeline.class
        );

        return build(graphStore, configuration, pipeline, progressTracker);
    }

    public NodeRegressionTrainAlgorithm build(
        GraphStore graphStore,
        NodeRegressionPipelineTrainConfig configuration,
        NodeRegressionTrainingPipeline pipeline,
        ProgressTracker progressTracker
    ) {
        validateMainMetric(pipeline, configuration.metrics().get(0).toString());

        var nodeFeatureProducer = NodeFeatureProducer.create(graphStore, configuration, executionContext, progressTracker);

        nodeFeatureProducer.validateNodePropertyStepsContextConfigs(pipeline.nodePropertySteps());

        return new NodeRegressionTrainAlgorithm(
            NodeRegressionTrain.create(
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
    public String taskName() {
        return "Node Regression Train Pipeline";
    }

    @Override
    public Task progressTask(GraphStore graphStore, NodeRegressionPipelineTrainConfig config) {
        var pipeline = PipelineCatalog.getTyped(
            config.username(),
            config.pipeline(),
            NodeRegressionTrainingPipeline.class
        );

        return progressTask(pipeline, graphStore.nodeCount());
    }

    public static Task progressTask(NodeRegressionTrainingPipeline pipeline, long nodeCount) {
        return NodeRegressionTrain.progressTask(pipeline, nodeCount);
    }
}
