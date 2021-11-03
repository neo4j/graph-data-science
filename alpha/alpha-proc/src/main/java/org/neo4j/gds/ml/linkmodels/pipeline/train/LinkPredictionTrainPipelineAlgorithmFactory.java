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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineUtils;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.List;

public class LinkPredictionTrainPipelineAlgorithmFactory extends AlgorithmFactory<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainConfig> {
    private final NamedDatabaseId databaseId;
    private final BaseProc caller;

    public LinkPredictionTrainPipelineAlgorithmFactory(NamedDatabaseId databaseId, BaseProc caller) {
        this.databaseId = databaseId;
        this.caller = caller;
    }

    @Override
    public LinkPredictionTrainPipelineExecutor build(
        Graph graph,
        LinkPredictionTrainConfig trainConfig,
        AllocationTracker allocationTracker,
        ProgressTracker progressTracker
    ) {
        String graphName = trainConfig
            .graphName()
            .orElseThrow(() -> new UnsupportedOperationException(
                "Link Prediction Pipeline cannot be used with anonymous graphs. Please load the graph before"));

        var graphStore = GraphStoreCatalog.get(CatalogRequest.of(trainConfig.username(), databaseId), graphName).graphStore();

        var pipeline = PipelineUtils.getPipelineModelInfo(trainConfig.pipeline(), trainConfig.username());
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
        return "Link Prediction pipeline train";
    }

    @Override
    public Task progressTask(Graph graph, LinkPredictionTrainConfig config) {
        var pipeline = PipelineUtils.getPipelineModelInfo(config.pipeline(), config.username());

        return Tasks.task(
            taskName(),
            Tasks.leaf("split relationships"),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                pipeline.nodePropertySteps().size()
            ),
            LinkPredictionTrain.progressTask(),
            Tasks.leaf("clean up[ graph store")
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(LinkPredictionTrainConfig configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
};
