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
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageResult;

import java.util.List;
import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.GraphSage;

public class GraphSageAlgorithmProcessing {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final NodeEmbeddingAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final GraphSageModelCatalog graphSageModelCatalog;

    public GraphSageAlgorithmProcessing(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        NodeEmbeddingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        GraphSageModelCatalog graphSageModelCatalog
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.graphSageModelCatalog = graphSageModelCatalog;
    }

    <CONFIGURATION extends GraphSageBaseConfig, RESULT, MUTATE_OR_WRITE_METADATA> RESULT process(
        GraphName graphName,
        CONFIGURATION configuration,
        Optional<MutateOrWriteStep<GraphSageResult, MUTATE_OR_WRITE_METADATA>> mutateOrWriteStep,
        ResultBuilder<CONFIGURATION, GraphSageResult, RESULT, MUTATE_OR_WRITE_METADATA> resultBuilder,
        boolean mutating
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
            () -> estimationFacade.graphSage(configuration, mutating),
            (graph, __) -> algorithms.graphSage(graph, configuration),
            mutateOrWriteStep,
            resultBuilder
        );
    }

   <CONFIGURATION extends GraphSageBaseConfig> GraphSageProcessParameters graphSageValidationHook( CONFIGURATION configuration){
       var model = graphSageModelCatalog.get(configuration);
       var relationshipWeightPropertyFromTrainConfiguration = model.trainConfig().relationshipWeightProperty();

       var validationHook = new GraphSageValidationHook(configuration, model);
        return new GraphSageProcessParameters(validationHook,relationshipWeightPropertyFromTrainConfiguration);
    }

    public record GraphSageProcessParameters(GraphSageValidationHook validationHook, Optional<String> relationshipWeightPropertyFromTrainConfiguration){}
}
