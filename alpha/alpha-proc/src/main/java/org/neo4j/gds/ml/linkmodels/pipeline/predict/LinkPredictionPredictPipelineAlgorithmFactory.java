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

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.similarity.knn.KnnFactory;

import java.util.List;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCompanion.getTrainedLPPipelineModel;

public class LinkPredictionPredictPipelineAlgorithmFactory<CONFIG extends LinkPredictionPredictPipelineBaseConfig> extends GraphStoreAlgorithmFactory<LinkPredictionPredictPipelineExecutor, CONFIG> {
    private final ExecutionContext executionContext;
    private final ModelCatalog modelCatalog;

    LinkPredictionPredictPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        super();
        this.executionContext = executionContext;
        this.modelCatalog = modelCatalog;
    }

    @Override
    public Task progressTask(GraphStore graphStore, CONFIG config) {
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
            ? Tasks.task("approximate link prediction", KnnFactory.knnTaskTree(graphStore.getUnion(), config.approximateConfig()))
            : Tasks.leaf("exhaustive link prediction", graphStore.nodeCount())
        );
    }

    @Override
    public String taskName() {
        return "Link Prediction Predict Pipeline";
    }

    @Override
    public LinkPredictionPredictPipelineExecutor build(
        GraphStore graphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var model = getTrainedLPPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var linkPredictionPipeline = model.customInfo().trainingPipeline();
        return new LinkPredictionPredictPipelineExecutor(
            linkPredictionPipeline,
            model.data(),
            configuration,
            executionContext,
            graphStore,
            configuration.graphName(),
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var model = getTrainedLPPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var linkPredictionPipeline = model.customInfo().trainingPipeline();
        var linkFeatureDimension = model.data().weights().data().totalSize();

        return LinkPredictionPredictPipelineExecutor.estimate(
            modelCatalog,
            linkPredictionPipeline,
            configuration,
            linkFeatureDimension
        );
    }
}
