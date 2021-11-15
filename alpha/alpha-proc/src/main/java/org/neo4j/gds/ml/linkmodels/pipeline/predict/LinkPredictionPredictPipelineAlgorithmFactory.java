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
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.similarity.knn.KnnFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.List;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.getTrainedLPPipelineModel;

public class LinkPredictionPredictPipelineAlgorithmFactory<CONFIG extends LinkPredictionPredictPipelineBaseConfig> extends AlgorithmFactory<LinkPredictionPredictPipelineExecutor, CONFIG> {
    private final BaseProc caller;
    private final NamedDatabaseId databaseId;
    private final ModelCatalog modelCatalog;

    LinkPredictionPredictPipelineAlgorithmFactory(
        BaseProc caller,
        NamedDatabaseId databaseId,
        ModelCatalog modelCatalog
    ) {
        super();
        this.caller = caller;
        this.databaseId = databaseId;
        this.modelCatalog = modelCatalog;
    }

    @Override
    protected Task progressTask(Graph graph, CONFIG config) {
        var trainingPipeline = getTrainedLPPipelineModel(modelCatalog, config.modelName(), config.username())
            .customInfo()
            .trainingPipeline();

        return Tasks.task(
            taskName(),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                trainingPipeline.nodePropertySteps().size()
            ),
            config.isApproximateStrategy()
                ? Tasks.task("approximate link prediction", KnnFactory.knnTaskTree(graph, config.approximateConfig()))
                : Tasks.leaf("exhaustive link prediction", graph.nodeCount()),
            Tasks.leaf("clean up graph store")
        );
    }

    @Override
    protected String taskName() {
        return "Link Prediction Predict Pipeline";
    }

    @Override
    protected LinkPredictionPredictPipelineExecutor build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        String graphName = configuration
            .graphName()
            .orElseThrow(() -> new UnsupportedOperationException(
                "Link Prediction Pipeline cannot be used with anonymous graphs. Please load the graph before"));

        var model = getTrainedLPPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var graphStore = GraphStoreCatalog.get(CatalogRequest.of(configuration.username(), databaseId), graphName).graphStore();
        var linkPredictionPipeline = model.customInfo().trainingPipeline();
        return new LinkPredictionPredictPipelineExecutor(
            linkPredictionPipeline,
            model.data(),
            configuration,
            caller,
            graphStore,
            graphName,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        throw new MemoryEstimationNotImplementedException();
    }
}
