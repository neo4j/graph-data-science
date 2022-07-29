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
package org.neo4j.gds.ml.pipeline.node.regression.predict;

import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionData;
import org.neo4j.gds.ml.models.linearregression.LinearRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorData;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;

public class NodeRegressionPredictPipelineAlgorithmFactory
    <CONFIG extends NodeRegressionPredictPipelineBaseConfig>
    extends GraphStoreAlgorithmFactory<NodeRegressionPredictPipelineExecutor, CONFIG>
{

    private final ModelCatalog modelCatalog;
    private final ExecutionContext executionContext;

    NodeRegressionPredictPipelineAlgorithmFactory(ExecutionContext executionContext, ModelCatalog modelCatalog) {
        super();
        this.modelCatalog = modelCatalog;
        this.executionContext = executionContext;
    }

    @Override
    public Task progressTask(GraphStore graphStore, CONFIG config) {
        var trainingPipeline = getTrainedNRPipelineModel(
            modelCatalog,
            config.modelName(),
            config.username()
        ).customInfo()
            .pipeline();

        return NodeRegressionPredictPipelineExecutor.progressTask(taskName(), trainingPipeline, graphStore);
    }

    @Override
    public String taskName() {
        return "Node Classification Predict Pipeline";
    }

    @Override
    public NodeRegressionPredictPipelineExecutor build(
        GraphStore graphStore,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var model = getTrainedNRPipelineModel(
            modelCatalog,
            configuration.modelName(),
            configuration.username()
        );
        var nodeClassificationPipeline = model.customInfo().pipeline();
        return new NodeRegressionPredictPipelineExecutor(
            nodeClassificationPipeline,
            configuration,
            executionContext,
            graphStore,
            progressTracker,
            regressorFrom(model.data())
        );
    }

    private static Regressor regressorFrom(
        Regressor.RegressorData regressorData
    ) {
        switch (regressorData.trainerMethod()) {
            case LinearRegression:
                return new LinearRegressor((LinearRegressionData) regressorData);
            case RandomForestRegression:
                return new RandomForestRegressor((RandomForestRegressorData) regressorData);
            default:
                throw new IllegalStateException("No such regressor: " + regressorData.trainerMethod().name());
        }
    }


    private static Model<Regressor.RegressorData, NodeRegressionPipelineTrainConfig, NodeRegressionPipelineModelInfo> getTrainedNRPipelineModel(
        ModelCatalog modelCatalog,
        String modelName,
        String username
    ) {
        return modelCatalog.get(
            username,
            modelName,
            Regressor.RegressorData.class,
            NodeRegressionPipelineTrainConfig.class,
            NodeRegressionPipelineModelInfo.class
        );
    }
}
