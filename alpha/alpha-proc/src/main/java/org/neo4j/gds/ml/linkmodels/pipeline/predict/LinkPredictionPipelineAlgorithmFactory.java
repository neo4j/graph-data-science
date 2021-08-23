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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.ml.linkmodels.pipeline.PipelineExecutor;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.List;

import static org.neo4j.gds.ml.linkmodels.pipeline.PipelineUtils.getLinkPredictionPipeline;

public class LinkPredictionPipelineAlgorithmFactory<CONFIG extends LinkPredictionPipelineBaseConfig> extends AlgorithmFactory<LinkPrediction, CONFIG> {
    private final BaseProc caller;
    private final NamedDatabaseId databaseId;

    LinkPredictionPipelineAlgorithmFactory(
        BaseProc caller,
        NamedDatabaseId databaseId
    ) {
        super();
        this.caller = caller;
        this.databaseId = databaseId;
    }

    @Override
    protected Task progressTask(Graph graph, CONFIG config) {
        var trainingPipeline = getLinkPredictionPipeline(config.modelName(), config.username())
            .customInfo()
            .trainingPipeline();

        return Tasks.task(
            taskName(),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                trainingPipeline.nodePropertySteps().size()
            ),
            Tasks.leaf("predict links", graph.nodeCount())
        );
    }

    @Override
    protected String taskName() {
        return "Link Prediction Pipeline";
    }

    @Override
    protected LinkPrediction build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
        String graphName = configuration
            .graphName()
            .orElseThrow(() -> new UnsupportedOperationException(
                "Link Prediction Pipeline cannot be used with anonymous graphs. Please load the graph before"));

        var model = getLinkPredictionPipeline(
            configuration.modelName(),
            configuration.username()
        );
        var graphStore = GraphStoreCatalog.get(configuration.username(), databaseId, graphName).graphStore();
        var pipelineExecutor = new PipelineExecutor(
            model.customInfo().trainingPipeline(),
            caller,
            databaseId,
            configuration.username(),
            graphName,
            progressTracker
        );
        var nodeLabels = configuration.nodeLabelIdentifiers(graphStore);
        var relationshipTypes = configuration.internalRelationshipTypes(graphStore);
        return new LinkPrediction(
            model.data(),
            pipelineExecutor,
            nodeLabels,
            relationshipTypes,
            graphStore,
            configuration.concurrency(),
            configuration.topN(),
            configuration.threshold(),
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
}
