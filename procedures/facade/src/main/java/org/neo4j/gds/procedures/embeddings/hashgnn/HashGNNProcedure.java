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
package org.neo4j.gds.procedures.embeddings.hashgnn;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMutateConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.DefaultNodeEmbeddingsComputationalResultTransformer;
import org.neo4j.gds.procedures.embeddings.HashGNNComputationalResultTransformer;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public class HashGNNProcedure {
    private final ConfigurationCreator configurationCreator;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade;

    public HashGNNProcedure(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
    }

    public Stream<HashGNNStreamResult> stream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, HashGNNStreamConfig::of);

        var computationResult = streamBusinessFacade.hashGNN(
            graphName,
            streamConfig
        );

        return HashGNNComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<DefaultNodeEmbeddingMutateResult> mutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, HashGNNMutateConfig::of);

        var computationResult = mutateBusinessFacade.hashGNN(
            graphName,
            mutateConfig
        );

        return Stream.of(DefaultNodeEmbeddingsComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, HashGNNStreamConfig::of);

        return Stream.of(estimateBusinessFacade.hashGNN(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, HashGNNMutateConfig::of);

        return Stream.of(estimateBusinessFacade.hashGNN(graphNameOrConfiguration, config));
    }
}
