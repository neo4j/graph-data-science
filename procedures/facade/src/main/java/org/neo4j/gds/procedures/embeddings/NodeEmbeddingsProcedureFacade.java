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
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsTrainBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMutateConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecWriteConfig;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.fastrp.FastRPStreamResult;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageStreamResult;
import org.neo4j.gds.procedures.embeddings.graphsage.GraphSageTrainResult;
import org.neo4j.gds.procedures.embeddings.hashgnn.HashGNNStreamResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecMutateResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecStreamResult;
import org.neo4j.gds.procedures.embeddings.node2vec.Node2VecWriteResult;
import org.neo4j.gds.procedures.embeddings.results.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.procedures.embeddings.results.DefaultNodeEmbeddingsWriteResult;
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
    private final NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade;

    private final NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade;

    // business logic

    public NodeEmbeddingsProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.streamBusinessFacade = streamBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.trainBusinessFacade = trainBusinessFacade;
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

    public Stream<DefaultNodeEmbeddingMutateResult> graphSageMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, GraphSageMutateConfig::of);

        var computationResult = mutateBusinessFacade.graphSage(
            graphName,
            mutateConfig
        );

        return Stream.of(DefaultNodeEmbeddingsComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> graphSageWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, GraphSageWriteConfig::of);

        var computationResult = writeBusinessFacade.graphSage(
            graphName,
            writeConfig
        );

        return Stream.of(DefaultNodeEmbeddingsComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> graphSageStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageStreamConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> graphSageMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageMutateConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> graphSageWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageWriteConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config));
    }

    public Stream<GraphSageTrainResult> graphSageTrain(
        String graphName,
        Map<String, Object> configuration
    ) {
        var trainConfig = configurationCreator.createConfigurationForStream(configuration, GraphSageTrainConfig::of);

        var computationResult = trainBusinessFacade.graphSage(
            graphName,
            trainConfig
        );

        return Stream.of(GraphSageComputationalResultTransformer.toTrainResult(computationResult));
    }

    public Stream<MemoryEstimateResult> graphSageTrainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageTrainConfig::of);

        return Stream.of(estimateBusinessFacade.graphSageTrain(graphNameOrConfiguration, config));
    }

    public Stream<FastRPStreamResult> fastRPStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, FastRPStreamConfig::of);

        var computationResult = streamBusinessFacade.fastRP(
            graphName,
            streamConfig
        );

        return FastRPComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<DefaultNodeEmbeddingMutateResult> fastRPMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, FastRPMutateConfig::of);

        var computationResult = mutateBusinessFacade.fastRP(
            graphName,
            mutateConfig
        );

        return Stream.of(DefaultNodeEmbeddingsComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<DefaultNodeEmbeddingsWriteResult> fastRPWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, FastRPWriteConfig::of);

        var computationResult = writeBusinessFacade.fastRP(
            graphName,
            writeConfig
        );

        return Stream.of(DefaultNodeEmbeddingsComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> fastRPStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, FastRPStreamConfig::of);

        return Stream.of(estimateBusinessFacade.fastRP(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> fastRPMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, FastRPMutateConfig::of);

        return Stream.of(estimateBusinessFacade.fastRP(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> fastRPWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, FastRPWriteConfig::of);

        return Stream.of(estimateBusinessFacade.fastRP(graphNameOrConfiguration, config));
    }

    public Stream<HashGNNStreamResult> HashGNNStream(
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

    public Stream<DefaultNodeEmbeddingMutateResult> HashGNNMutate(
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

    public Stream<MemoryEstimateResult> HashGNNStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, HashGNNStreamConfig::of);

        return Stream.of(estimateBusinessFacade.hashGNN(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> HashGNNMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, HashGNNMutateConfig::of);

        return Stream.of(estimateBusinessFacade.hashGNN(graphNameOrConfiguration, config));
    }


}
