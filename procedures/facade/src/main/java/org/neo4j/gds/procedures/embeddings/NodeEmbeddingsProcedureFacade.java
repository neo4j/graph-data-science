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
package org.neo4j.gds.procedures.embeddings;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageStreamResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecMutateResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecStreamResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecWriteResult;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public class NodeEmbeddingsProcedureFacade {
    // services
    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade;
    private final NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade;

    // business logic

    public NodeEmbeddingsProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.streamBusinessFacade = streamBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
        this.estimateBusinessFacade = estimateBusinessFacade;
    }

    public Stream<Node2VecStreamResult> node2VecStream(
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

    public Stream<Node2VecMutateResult> node2VecMutate(
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

    public Stream<Node2VecWriteResult> node2VecWrite(
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

    public Stream<MemoryEstimateResult> node2VecStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecStreamConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> node2VecMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecMutateConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> node2VecWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, Node2VecWriteConfig::of);

        return Stream.of(estimateBusinessFacade.node2Vec(graphNameOrConfiguration, config));
    }

    public Stream<GraphSageStreamResult> graphSageStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, GraphSageStreamConfig::of);

        var computationResult = streamBusinessFacade.graphSage(
            graphName,
            streamConfig
        );

        return GraphSageComputationalResultTransformer.toStreamResult(computationResult);
    }

}
