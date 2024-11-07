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
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.GraphSageTrain;

public class NodeEmbeddingAlgorithmsTrainModeBusinessFacade {
    private final GraphSageModelCatalog graphSageModelCatalog;
    private final ModelRepository modelRepository;
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    NodeEmbeddingAlgorithmsTrainModeBusinessFacade(
        GraphSageModelCatalog graphSageModelCatalog,
        ModelRepository modelRepository,
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.graphSageModelCatalog = graphSageModelCatalog;
        this.modelRepository = modelRepository;
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT graphSage(
        GraphName graphName,
        GraphSageTrainConfig configuration,
        ResultBuilder<GraphSageTrainConfig, Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics>, RESULT, Void> resultBuilder
    ) {
        var validationHook = new GraphSageTrainValidationHook(configuration);

        var writeToDiskStep = new GraphSageTrainWriteToDiskStep(
            graphSageModelCatalog,
            modelRepository,
            configuration
        );

        return algorithmProcessingTemplateConvenience.processAlgorithmInWriteMode(
            Optional.empty(),
            graphName,
            configuration,
            Optional.of(List.of(validationHook)),
            Optional.empty(),
            GraphSageTrain,
            () -> estimation.graphSageTrain(configuration),
            (graph, __) -> algorithms.graphSageTrain(graph, configuration),
            writeToDiskStep,
            resultBuilder
        );
    }
}
