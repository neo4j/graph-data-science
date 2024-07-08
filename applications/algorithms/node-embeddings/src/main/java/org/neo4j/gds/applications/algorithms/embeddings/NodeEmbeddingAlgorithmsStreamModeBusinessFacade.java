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

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FastRP;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.GraphSage;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HashGNN;

public class NodeEmbeddingAlgorithmsStreamModeBusinessFacade {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    public NodeEmbeddingAlgorithmsStreamModeBusinessFacade(
        GraphSageModelCatalog graphSageModelCatalog,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPStreamConfig configuration,
        ResultBuilder<FastRPStreamConfig, FastRPResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            FastRP,
            () -> estimationFacade.fastRP(configuration),
            graph -> algorithms.fastRP(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT graphSage(
        GraphName graphName,
        GraphSageStreamConfig configuration,
        ResultBuilder<GraphSageStreamConfig, GraphSageResult, RESULT, Void> resultBuilder
    ) {
        var model = graphSageModelCatalog.get(configuration);
        var relationshipWeightPropertyFromTrainConfiguration = model.trainConfig().relationshipWeightProperty();

        var validationHook = new GraphSageValidationHook(configuration, model);

        return algorithmProcessingTemplateConvenience.processAlgorithm(
            relationshipWeightPropertyFromTrainConfiguration,
            graphName,
            configuration,
            Optional.of(List.of(validationHook)),
            GraphSage,
            () -> estimationFacade.graphSage(configuration, false),
            graph -> algorithms.graphSage(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT hashGnn(
        GraphName graphName,
        HashGNNStreamConfig configuration,
        ResultBuilder<HashGNNStreamConfig, HashGNNResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            HashGNN,
            () -> estimationFacade.hashGnn(configuration),
            graph -> algorithms.hashGnn(graph, configuration),
            resultBuilder
        );
    }
}
