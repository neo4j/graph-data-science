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

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.validation.AfterLoadValidation;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageModelResolver;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainAlgorithmFactory;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNFactory;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecAlgorithmFactory;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.modelcatalogservices.ModelCatalogService;

import java.util.List;
import java.util.Optional;

public class NodeEmbeddingsAlgorithmsFacade {

    private final AlgorithmRunner algorithmRunner;
    private final ModelCatalogService modelCatalogService;

    public NodeEmbeddingsAlgorithmsFacade(
        AlgorithmRunner algorithmRunner,
        ModelCatalogService modelCatalogService
    ) {
        this.algorithmRunner = algorithmRunner;
        this.modelCatalogService = modelCatalogService;
    }

    AlgorithmComputationResult<Node2VecResult> node2Vec(
        String graphName,
        Node2VecBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new Node2VecAlgorithmFactory<>()
        );
    }

    AlgorithmComputationResult<GraphSageResult> graphSage(
        String graphName,
        GraphSageBaseConfig config
    ) {
        Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> model = GraphSageModelResolver.resolveModel(
            modelCatalogService.get(),
            config.username(),
            config.modelName()
        );

        AfterLoadValidation validationCondition = (graphStore) -> {
            GraphSageTrainConfig trainConfig = model.trainConfig();
            trainConfig.graphStoreValidation(
                graphStore,
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            );
        };
        
        return algorithmRunner.run(
            graphName,
            config,
            model.trainConfig().relationshipWeightProperty(),
            new GraphSageAlgorithmFactory<>(modelCatalogService.get()),
            List.of(validationCondition)
        );
    }

    AlgorithmComputationResult<Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>> graphSageTrain(
        String graphName,
        GraphSageTrainConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new GraphSageTrainAlgorithmFactory()
        );
    }

    AlgorithmComputationResult<HashGNNResult> hashGNN(
        String graphName,
        HashGNNConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            Optional.empty(),
            new HashGNNFactory<>()
        );
    }

}
