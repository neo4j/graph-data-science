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
package org.neo4j.gds.procedures.embeddings.graphsage;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsTrainBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.DefaultNodeEmbeddingsComputationalResultTransformer;
import org.neo4j.gds.procedures.embeddings.GraphSageComputationalResultTransformer;
import org.neo4j.gds.procedures.embeddings.results.DefaultNodeEmbeddingMutateResult;
import org.neo4j.gds.procedures.embeddings.results.DefaultNodeEmbeddingsWriteResult;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public class GraphSageProcedure {
    private final ConfigurationCreator configurationCreator;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade;
    private final NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade;
    private final NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade;

    public GraphSageProcedure(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade= writeBusinessFacade;
        this.trainBusinessFacade=trainBusinessFacade;
    }

    public Stream<GraphSageStreamResult> stream(
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

    public Stream<DefaultNodeEmbeddingMutateResult> mutate(
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

    public Stream<DefaultNodeEmbeddingsWriteResult> write(
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

    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageStreamConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config, false));
    }

    public Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageMutateConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config, true));
    }

    public Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageWriteConfig::of);

        return Stream.of(estimateBusinessFacade.graphSage(graphNameOrConfiguration, config, false));
    }

    public Stream<GraphSageTrainResult> train(
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

    public Stream<MemoryEstimateResult> trainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, GraphSageTrainConfig::of);

        return Stream.of(estimateBusinessFacade.graphSageTrain(graphNameOrConfiguration, config));
    }
}
