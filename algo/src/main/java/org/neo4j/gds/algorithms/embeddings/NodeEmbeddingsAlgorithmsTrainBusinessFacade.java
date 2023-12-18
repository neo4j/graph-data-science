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
package org.neo4j.gds.algorithms.embeddings;

import org.neo4j.gds.algorithms.TrainResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.modelcatalogservices.ModelCatalogService;

public class NodeEmbeddingsAlgorithmsTrainBusinessFacade {

    private final NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade;
    private final ModelCatalogService modelCatalogService;


    public NodeEmbeddingsAlgorithmsTrainBusinessFacade(NodeEmbeddingsAlgorithmsFacade nodeEmbeddingsAlgorithmsFacade,
        ModelCatalogService modelCatalogService
    ) {
        this.nodeEmbeddingsAlgorithmsFacade = nodeEmbeddingsAlgorithmsFacade;
        this.modelCatalogService = modelCatalogService;
    }

    public TrainResult<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>> graphSage(
        String graphName,
        GraphSageTrainConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> nodeEmbeddingsAlgorithmsFacade.graphSageTrain(graphName, configuration)
        );

        var model = intermediateResult.algorithmResult.result().orElse(null);
            if (model!=null){

                modelCatalogService.set(model);
                if (configuration.storeModelToDisk()){
                    modelCatalogService.storeModelToDisk(model);
                }

            }
            return TrainResult.<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>>builder().trainMillis(intermediateResult.computeMilliseconds)
                .algorithmSpecificFields(model)
                .build();


    }

}
