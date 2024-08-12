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
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FastRP;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HashGNN;

public class NodeEmbeddingAlgorithmsStreamModeBusinessFacade {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final GraphSageAlgorithmProcessing graphSageAlgorithmProcessing;
    private final Node2VecAlgorithmProcessing node2VecAlgorithmProcessing;

    NodeEmbeddingAlgorithmsStreamModeBusinessFacade(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        GraphSageAlgorithmProcessing graphSageAlgorithmProcessing,
        Node2VecAlgorithmProcessing node2VecAlgorithmProcessing
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.graphSageAlgorithmProcessing = graphSageAlgorithmProcessing;
        this.node2VecAlgorithmProcessing = node2VecAlgorithmProcessing;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPStreamConfig configuration,
        ResultBuilder<FastRPStreamConfig, FastRPResult, RESULT, Void> resultBuilder
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

    public <RESULT> RESULT graphSage(
        GraphName graphName,
        GraphSageStreamConfig configuration,
        ResultBuilder<GraphSageStreamConfig, GraphSageResult, RESULT, Void> resultBuilder
    ) {
        return graphSageAlgorithmProcessing.process(graphName, configuration, Optional.empty(), resultBuilder, false);
    }

    public <RESULT> RESULT hashGnn(
        GraphName graphName,
        HashGNNStreamConfig configuration,
        ResultBuilder<HashGNNStreamConfig, HashGNNResult, RESULT, Void> resultBuilder
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

    public <RESULT> RESULT node2Vec(
        GraphName graphName,
        Node2VecStreamConfig configuration,
        ResultBuilder<Node2VecStreamConfig, Node2VecResult, RESULT, Void> resultBuilder
    ) {
        return node2VecAlgorithmProcessing.process(graphName, configuration, Optional.empty(), resultBuilder);
    }
}
