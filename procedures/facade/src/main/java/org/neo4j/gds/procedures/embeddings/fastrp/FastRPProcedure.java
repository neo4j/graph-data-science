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
package org.neo4j.gds.procedures.embeddings.fastrp;

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmStreamBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.DefaultNodeEmbeddingsComputationalResultTransformer;
import org.neo4j.gds.procedures.embeddings.FastRPComputationalResultTransformer;
import org.neo4j.gds.procedures.embeddings.results.DefaultNodeEmbeddingsWriteResult;

import java.util.Map;
import java.util.stream.Stream;

public class FastRPProcedure {
    private final ConfigurationCreator configurationCreator;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade;

    private final NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade;

    public FastRPProcedure(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmStreamBusinessFacade streamBusinessFacade,
        NodeEmbeddingsAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade= writeBusinessFacade;
    }

    public Stream<FastRPStreamResult> stream(
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

    public Stream<DefaultNodeEmbeddingsWriteResult> write(
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

    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, FastRPStreamConfig::of);

        return Stream.of(estimateBusinessFacade.fastRP(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, FastRPWriteConfig::of);

        return Stream.of(estimateBusinessFacade.fastRP(graphNameOrConfiguration, config));
    }
}
