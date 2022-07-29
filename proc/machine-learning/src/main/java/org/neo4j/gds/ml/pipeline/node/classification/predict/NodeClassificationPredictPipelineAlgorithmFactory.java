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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

public class NodeClassificationPredictPipelineAlgorithmFactory
    <CONFIG extends NodeClassificationPredictPipelineBaseConfig>
    extends GraphStoreAlgorithmFactory<NodeClassificationPredictPipelineExecutor, CONFIG>
{

    private final ModelCatalog modelCatalog;
    private final ExecutionContext executionContext;

    NodeClassificationPredictPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        super();
        this.modelCatalog = modelCatalog;
        this.executionContext = executionContext;
    }

    @Override
    public Task progressTask(GraphStore graphStore, CONFIG config) {
        var trainingPipeline = getTrainedNCPipelineModel(
            modelCatalog,
            config.modelName(),
            config.username()
        ).customInfo().pipeline();

        return NodeClassificationPredictPipelineExecutor.progressTask(taskName(), trainingPipeline, graphStore);
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
        var nodeClassificationPipeline = model.customInfo().pipeline();
        var classIdMap = LocalIdMap.of(model.customInfo().classes());
        return new NodeClassificationPredictPipelineExecutor(
            nodeClassificationPipeline,
            configuration,
            executionContext,
            graphStore,
            progressTracker,
            model.data(),
            classIdMap
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        var model = getTrainedNCPipelineModel(
            this.modelCatalog,
            configuration.modelName(),
            configuration.username()
        );

        return MemoryEstimations.builder(NodeClassificationPredictPipelineExecutor.class.getSimpleName())
            .add("Pipeline executor", NodeClassificationPredictPipelineExecutor.estimate(model, configuration, modelCatalog))
            .build();
    }

    private static Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> getTrainedNCPipelineModel(
        ModelCatalog modelCatalog,
        String modelName,
        String username
    ) {
        return modelCatalog.get(
            username,
            modelName,
            Classifier.ClassifierData.class,
            NodeClassificationPipelineTrainConfig.class,
            NodeClassificationPipelineModelInfo.class
        );
    }
}
