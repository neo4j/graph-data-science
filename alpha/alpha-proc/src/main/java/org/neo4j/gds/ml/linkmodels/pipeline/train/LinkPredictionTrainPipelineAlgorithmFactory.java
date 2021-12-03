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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion;

import java.util.List;

public class LinkPredictionTrainPipelineAlgorithmFactory extends GraphStoreAlgorithmFactory<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainConfig> {
    private final BaseProc caller;
    private final ModelCatalog modelCatalog;

    public LinkPredictionTrainPipelineAlgorithmFactory(BaseProc caller, ModelCatalog modelCatalog) {
        this.caller = caller;
        this.modelCatalog = modelCatalog;
    }

    @Override
    public LinkPredictionTrainPipelineExecutor build(
        Graph graph,
        GraphStore graphStore,
        LinkPredictionTrainConfig trainConfig,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        String graphName = ""; // TODO: fixme

        var pipeline = LinkPredictionPipelineCompanion.getLPPipeline(modelCatalog, trainConfig.pipeline(), trainConfig.username());
        pipeline.validate();

        return new LinkPredictionTrainPipelineExecutor(
            pipeline,
            trainConfig,
            caller,
            graphStore,
            graphName,
            progressTracker
        );
    }

    @Override
    protected String taskName() {
        return "Link Prediction Train Pipeline";
    }

    @Override
    public Task progressTask(Graph graph, LinkPredictionTrainConfig config) {
        var pipeline = LinkPredictionPipelineCompanion.getLPPipeline(modelCatalog, config.pipeline(), config.username());

        return Tasks.task(
            taskName(),
            Tasks.leaf("split relationships"),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                pipeline.nodePropertySteps().size()
            ),
            LinkPredictionTrain.progressTask()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(LinkPredictionTrainConfig configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
};
