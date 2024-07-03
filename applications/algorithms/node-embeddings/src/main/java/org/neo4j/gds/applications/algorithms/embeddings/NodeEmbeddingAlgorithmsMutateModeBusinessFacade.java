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

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FastRP;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.GraphSage;

public class NodeEmbeddingAlgorithmsMutateModeBusinessFacade {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    public NodeEmbeddingAlgorithmsMutateModeBusinessFacade(
        GraphSageModelCatalog graphSageModelCatalog,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT fastRP(
        GraphName graphName,
        FastRPMutateConfig configuration,
        ResultBuilder<FastRPMutateConfig, FastRPResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new FastRPMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            FastRP,
            () -> estimation.fastRP(configuration),
            graph -> algorithms.fastRP(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT graphSage(
        GraphName graphName,
        GraphSageMutateConfig configuration,
        ResultBuilder<GraphSageMutateConfig, GraphSageResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var model = graphSageModelCatalog.get(configuration);
        var relationshipWeightPropertyFromTrainConfiguration = model.trainConfig().relationshipWeightProperty();

        var validationHook = new GraphSageValidationHook(configuration, model);

        var mutateStep = new GraphSageMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processAlgorithm(
            relationshipWeightPropertyFromTrainConfiguration,
            graphName,
            configuration,
            Optional.of(List.of(validationHook)),
            GraphSage,
            () -> estimation.graphSage(configuration, true),
            graph -> algorithms.graphSage(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }
}
