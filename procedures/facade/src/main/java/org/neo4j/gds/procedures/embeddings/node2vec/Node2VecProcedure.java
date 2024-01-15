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
package org.neo4j.gds.procedures.embeddings.node2vec;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.Node2VecComputationalResultTransformer;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public class Node2VecProcedure {
    private final ConfigurationCreator configurationCreator;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade;

    private final NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade;

    public Node2VecProcedure(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade= writeBusinessFacade;
    }

    public Stream<Node2VecStreamResult> stream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, Node2VecStreamConfig::of);

        var computationResult = streamBusinessFacade.node2Vec(
            graphName,
            streamConfig
        );

        return Node2VecComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<Node2VecMutateResult> mutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, Node2VecMutateConfig::of);

        var computationResult = mutateBusinessFacade.node2Vec(
            graphName,
            mutateConfig
        );

        return Stream.of(Node2VecComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<Node2VecWriteResult> write(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, Node2VecWriteConfig::of);

        var computationResult = writeBusinessFacade.node2Vec(
            graphName,
            writeConfig
        );

        return Stream.of(Node2VecComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecStreamConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecMutateConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecWriteConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }
}
