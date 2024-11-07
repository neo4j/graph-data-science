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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.FastRP;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.GraphSage;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.HashGNN;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Node2Vec;

public class NodeEmbeddingAlgorithmsStreamModeBusinessFacade {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final GraphSageAlgorithmProcessing graphSageAlgorithmProcessing;

    NodeEmbeddingAlgorithmsStreamModeBusinessFacade(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        GraphSageAlgorithmProcessing graphSageAlgorithmProcessing
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.graphSageAlgorithmProcessing = graphSageAlgorithmProcessing;
    }

    public <RESULT> Stream<RESULT> fastRP(
        GraphName graphName,
        FastRPStreamConfig configuration,
        StreamResultBuilder<FastRPResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            FastRP,
            () -> estimationFacade.fastRP(configuration),
            (graph, __) -> algorithms.fastRP(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> graphSage(
        GraphName graphName,
        GraphSageStreamConfig configuration,
        StreamResultBuilder<GraphSageResult, RESULT> resultBuilder
    ) {

        var graphSageProcessParameters = graphSageAlgorithmProcessing.graphSageValidationHook(configuration);
        return algorithmProcessingTemplateConvenience.processAlgorithmInStreamMode(
            graphName,
            configuration,
            GraphSage,
            () -> estimationFacade.graphSage(configuration, false),
            (graph, __) -> algorithms.graphSage(graph, configuration),
            resultBuilder,
            Optional.of(List.of(graphSageProcessParameters.validationHook())),
            Optional.empty(),
            graphSageProcessParameters.relationshipWeightPropertyFromTrainConfiguration()
        );
    }

    public <RESULT> Stream<RESULT> hashGnn(
        GraphName graphName,
        HashGNNStreamConfig configuration,
        StreamResultBuilder<HashGNNResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            HashGNN,
            () -> estimationFacade.hashGnn(configuration),
            (graph, __) -> algorithms.hashGnn(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> node2Vec(
        GraphName graphName,
        Node2VecStreamConfig configuration,
        StreamResultBuilder<Node2VecResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processAlgorithmInStreamMode(
            graphName,
            configuration,
            Node2Vec,
            () -> estimationFacade.node2Vec(configuration),
            (graph, __) -> algorithms.node2Vec(graph, configuration),
            resultBuilder,
            Optional.of(List.of(new Node2VecValidationHook(configuration))),
            Optional.empty(),
            Optional.empty()
        );
    }
}
