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

import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.embeddings.NodeEmbeddingsAlgorithmsTrainBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.embeddings.GraphSageComputationalResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class GraphSageProcedure {
    private final ConfigurationCreator configurationCreator;
    private final NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade;

    public GraphSageProcedure(
        ConfigurationCreator configurationCreator,
        NodeEmbeddingsAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        NodeEmbeddingsAlgorithmsTrainBusinessFacade trainBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.trainBusinessFacade=trainBusinessFacade;
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
