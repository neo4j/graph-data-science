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

import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainEstimateDefinition;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMemoryEstimateDefinition;
import org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecMemoryEstimateDefinition;

import java.util.Optional;

public class NodeEmbeddingsAlgorithmsEstimateBusinessFacade {

    private final AlgorithmEstimator algorithmEstimator;


    public NodeEmbeddingsAlgorithmsEstimateBusinessFacade(
        AlgorithmEstimator algorithmEstimator
    ) {
        this.algorithmEstimator = algorithmEstimator;
    }

    public <C extends Node2VecBaseConfig> MemoryEstimateResult node2Vec(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new Node2VecMemoryEstimateDefinition(configuration.node2VecParameters())
        );
    }

    public <C extends GraphSageTrainConfig> MemoryEstimateResult graphSageTrain(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            configuration.relationshipWeightProperty(),
            new GraphSageTrainEstimateDefinition(configuration.toMemoryEstimateParameters())
        );
    }

    public <C extends HashGNNConfig> MemoryEstimateResult hashGNN(
        Object graphNameOrConfiguration,
        C configuration
    ) {
        return algorithmEstimator.estimate(
            graphNameOrConfiguration,
            configuration,
            Optional.empty(),
            new HashGNNMemoryEstimateDefinition(configuration.toParameters())
        );
    }
}
