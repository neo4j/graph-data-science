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
package org.neo4j.gds.applications.algorithms.machinelearning;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictMutateConfig;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KGE;

public class MachineLearningAlgorithmsMutateModeBusinessFacade {
    private final RequestScopedDependencies requestScopedDependencies;
    private final MachineLearningAlgorithmsEstimationModeBusinessFacade estimation;
    private final MachineLearningAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    MachineLearningAlgorithmsMutateModeBusinessFacade(
        RequestScopedDependencies requestScopedDependencies,
        MachineLearningAlgorithmsEstimationModeBusinessFacade estimation,
        MachineLearningAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.requestScopedDependencies = requestScopedDependencies;
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT kge(
        GraphName graphName,
        KGEPredictMutateConfig configuration,
        ResultBuilder<KGEPredictMutateConfig, KGEPredictResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new KgeMutateStep(requestScopedDependencies.getTerminationFlag(), configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            KGE,
            estimation::kge,
            (graph, __) -> algorithms.kge(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
