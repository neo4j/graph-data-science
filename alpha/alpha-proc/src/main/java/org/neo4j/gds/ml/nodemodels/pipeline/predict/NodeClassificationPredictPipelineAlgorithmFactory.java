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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictAlgorithmFactory;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictConfig;
import org.neo4j.gds.ml.nodemodels.NodeClassificationPredictConfigImpl;

import java.util.List;

import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.getTrainedNCPipelineModel;

public class NodeClassificationPredictPipelineAlgorithmFactory
    <CONFIG extends NodeClassificationPredictPipelineBaseConfig>
    extends GraphStoreAlgorithmFactory<NodeClassificationPredictPipelineExecutor, CONFIG>
{

    private final ModelCatalog modelCatalog;
    private final ExecutionContext executionContext;
    private final NodeClassificationPredictAlgorithmFactory<NodeClassificationPredictConfig> innerFactory;

    NodeClassificationPredictPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        super();
        this.modelCatalog = modelCatalog;
        this.innerFactory = new NodeClassificationPredictAlgorithmFactory<>(modelCatalog);
        this.executionContext = executionContext;
    }

    @Override
    public Task progressTask(GraphStore graphStore, CONFIG config) {
        var trainingPipeline = getTrainedNCPipelineModel(
            modelCatalog,
            config.modelName(),
            config.username()
        ).customInfo()
            .trainingPipeline();

        return Tasks.task(
            taskName(),
            Tasks.iterativeFixed(
                "execute node property steps",
                () -> List.of(Tasks.leaf("step")),
                trainingPipeline.nodePropertySteps().size()
            ),
            innerFactory.progressTask(graphStore.getUnion(), innerConfig(config))
        );
    }

    private NodeClassificationPredictConfig innerConfig(CONFIG configuration) {
        return new NodeClassificationPredictConfigImpl(
            configuration.username(),
            CypherMapWrapper.create(configuration.toMap())
                .withEntry("includePredictedProbabilities",configuration.includePredictedProbabilities())
                .withoutEntry("predictedProbabilityProperty")
            );
    }

    @Override
    public String taskName() {
        return "Node Classification Predict Pipeline";
    }

    @Override
    public NodeClassificationPredictPipelineExecutor build(
        GraphStore graphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var model = getTrainedNCPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var nodeClassificationPipeline = model.customInfo().trainingPipeline();
        return new NodeClassificationPredictPipelineExecutor(
            nodeClassificationPipeline,
            configuration,
            executionContext,
            graphStore,
            configuration.graphName(),
            progressTracker,
            model.data()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var model = getTrainedNCPipelineModel(
            this.modelCatalog,
            configuration.modelName(),
            configuration.username()
        );

        return MemoryEstimations.builder(NodeClassificationPredictPipelineExecutor.class)
            .add("Pipeline executor", NodeClassificationPredictPipelineExecutor.estimate(model, configuration, modelCatalog))
            .build();
    }
}
