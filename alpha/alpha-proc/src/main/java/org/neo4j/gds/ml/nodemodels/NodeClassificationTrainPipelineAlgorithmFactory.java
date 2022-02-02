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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationTrainPipelineExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;

import java.util.List;

public class NodeClassificationTrainPipelineAlgorithmFactory extends GraphStoreAlgorithmFactory<NodeClassificationTrainPipelineExecutor, NodeClassificationPipelineTrainConfig> {

    private final ExecutionContext executionContext;
    private final ModelCatalog modelCatalog;

    public NodeClassificationTrainPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        this.executionContext = executionContext;
        this.modelCatalog = modelCatalog;
    }

    @Override
    public NodeClassificationTrainPipelineExecutor build(
        GraphStore graphStore,
        NodeClassificationPipelineTrainConfig configuration,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        var pipeline = NodeClassificationPipelineCompanion.getNCPipeline(
            this.modelCatalog,
            configuration.pipeline(),
            configuration.username()
        );
        pipeline.validateBeforeExecution(graphStore, configuration);

        return new NodeClassificationTrainPipelineExecutor(
            pipeline,
            configuration,
            executionContext,
            graphStore,
            configuration.graphName(),
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(NodeClassificationPipelineTrainConfig configuration) {
        var pipeline = NodeClassificationPipelineCompanion.getNCPipeline(
            this.modelCatalog,
            configuration.pipeline(),
            configuration.username()
        );

        return MemoryEstimations.builder(NodeClassificationTrainPipelineExecutor.class)
            .add("Pipeline executor", NodeClassificationTrainPipelineExecutor.estimate(pipeline, configuration))
            .build();
    }

    @Override
    public String taskName() {
        return "Node Classification Train Pipeline";
    }

    @Override
    public Task progressTask(GraphStore graphStore, NodeClassificationPipelineTrainConfig config) {
        var pipeline = NodeClassificationPipelineCompanion.getNCPipeline(
            this.modelCatalog,
            config.pipeline(),
            config.username()
        );

        return Tasks.task(
            taskName(),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                pipeline.nodePropertySteps().size()
            ),
            NodeClassificationTrain.progressTask(
                pipeline.splitConfig().validationFolds(),
                pipeline.trainingParameterSpace().size()
            )
        );
    }
}
