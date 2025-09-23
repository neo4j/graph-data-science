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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPConfigTransformer;
import org.neo4j.gds.embeddings.fastrp.FastRPMemoryEstimateDefinition;
import org.neo4j.gds.embeddings.graphsage.TrainConfigTransformer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMemoryEstimateDefinition;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainEstimateDefinition;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainMemoryEstimateParameters;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfigTransformer;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMemoryEstimateDefinition;
import org.neo4j.gds.embeddings.hashgnn.HashGNNParameters;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecConfigTransformer;
import org.neo4j.gds.embeddings.node2vec.Node2VecMemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.model.ModelConfig;

public class NodeEmbeddingAlgorithmsEstimationModeBusinessFacade {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public NodeEmbeddingAlgorithmsEstimationModeBusinessFacade(
        GraphSageModelCatalog graphSageModelCatalog,
        AlgorithmEstimationTemplate algorithmEstimationTemplate
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimation fastRP(FastRPBaseConfig configuration) {
        return new FastRPMemoryEstimateDefinition(FastRPConfigTransformer.toParameters(configuration)).memoryEstimation();
    }

    public MemoryEstimateResult fastRP(FastRPBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = fastRP(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation graphSage(ModelConfig configuration, boolean mutating) {
        var model = graphSageModelCatalog.get(configuration.username(), configuration.modelName());

        var memoryEstimateParameters = TrainConfigTransformer.toMemoryEstimateParameters(model.trainConfig());

        return new GraphSageMemoryEstimateDefinition(memoryEstimateParameters, mutating).memoryEstimation();
    }

    public MemoryEstimateResult graphSage(GraphSageBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = graphSage(configuration, false);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    MemoryEstimation graphSageTrain(GraphSageTrainConfig configuration) {
        var parameters = TrainConfigTransformer.toMemoryEstimateParameters(configuration);

        return graphSageTrain(parameters);
    }

    MemoryEstimation graphSageTrain(GraphSageTrainMemoryEstimateParameters parameters) {
        return new GraphSageTrainEstimateDefinition(parameters).memoryEstimation();
    }

    public MemoryEstimateResult graphSageTrain(GraphSageTrainConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = graphSageTrain(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation hashGnn(HashGNNConfig configuration) {
        var parameters = HashGNNConfigTransformer.toParameters(configuration);

        return hashGnn(parameters);
    }

    private MemoryEstimation hashGnn(HashGNNParameters parameters) {
        return new HashGNNMemoryEstimateDefinition(parameters).memoryEstimation();
    }

    public MemoryEstimateResult hashGnn(HashGNNConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = hashGnn(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation node2Vec(Node2VecBaseConfig configuration) {
        return new Node2VecMemoryEstimateDefinition(Node2VecConfigTransformer.toParameters(configuration)).memoryEstimation();
    }

    public MemoryEstimateResult node2Vec(Node2VecBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = node2Vec(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }
}
