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

public class NodeRegressionTrainPipelineAlgorithmFactory extends GraphStoreAlgorithmFactory<NodeRegressionTrainPipelineExecutor, NodeRegressionPipelineTrainConfig> {

    private final ExecutionContext executionContext;

    public NodeRegressionTrainPipelineAlgorithmFactory(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public NodeRegressionTrainPipelineExecutor build(
        GraphStore graphStore,
        NodeRegressionPipelineTrainConfig configuration,
        ProgressTracker progressTracker
    ) {
        var pipeline = PipelineCatalog.getTyped(
            configuration.username(),
            configuration.pipeline(),
            NodeRegressionTrainingPipeline.class
        );

        return new NodeRegressionTrainPipelineExecutor(
            pipeline,
            configuration,
            executionContext,
            graphStore,
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

        return NodeRegressionTrainPipelineExecutor.progressTask(pipeline, graphStore.nodeCount());
    }
}
