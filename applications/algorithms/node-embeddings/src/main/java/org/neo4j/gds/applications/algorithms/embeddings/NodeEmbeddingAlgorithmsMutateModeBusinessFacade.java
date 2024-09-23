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
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNMutateConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecMutateConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.FastRP;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.GraphSage;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.HashGNN;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Node2Vec;

public class NodeEmbeddingAlgorithmsMutateModeBusinessFacade {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;
    private final GraphSageAlgorithmProcessing graphSageAlgorithmProcessing;

    public NodeEmbeddingAlgorithmsMutateModeBusinessFacade(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty,
        GraphSageAlgorithmProcessing graphSageAlgorithmProcessing) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
        this.graphSageAlgorithmProcessing = graphSageAlgorithmProcessing;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPMutateConfig configuration,
        ResultBuilder<FastRPMutateConfig, FastRPResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new FastRPMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            FastRP,
            () -> estimation.fastRP(configuration),
            (graph, __) -> algorithms.fastRP(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT graphSage(
        GraphName graphName,
        GraphSageMutateConfig configuration,
        ResultBuilder<GraphSageMutateConfig, GraphSageResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new GraphSageMutateStep(mutateNodeProperty, configuration);
        var graphSageProcessParameters = graphSageAlgorithmProcessing.graphSageValidationHook(configuration);

        return algorithmProcessingTemplateConvenience.processAlgorithmInMutateMode(
            graphSageProcessParameters.relationshipWeightPropertyFromTrainConfiguration(),
            graphName,
            configuration,
            Optional.of(List.of(graphSageProcessParameters.validationHook())),
            GraphSage,
            () -> estimation.graphSage(configuration, true),
            (graph, __) -> algorithms.graphSage(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT hashGnn(
        GraphName graphName,
        HashGNNMutateConfig configuration,
        ResultBuilder<HashGNNMutateConfig, HashGNNResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HashGnnMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            HashGNN,
            () -> estimation.hashGnn(configuration),
            (graph, __) -> algorithms.hashGnn(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT node2Vec(
        GraphName graphName,
        Node2VecMutateConfig configuration,
        ResultBuilder<Node2VecMutateConfig, Node2VecResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new Node2VecMutateStep(mutateNodeProperty, configuration);
        var validationHook = new Node2VecValidationHook(configuration);

        return algorithmProcessingTemplateConvenience.processAlgorithmInMutateMode(
            Optional.empty(),
            graphName,
            configuration,
            Optional.of(List.of(validationHook)),
            Node2Vec,
            () -> estimation.node2Vec(configuration),
            (graph, __) -> algorithms.node2Vec(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
