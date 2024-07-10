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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ScaleProperties;

public class MiscellaneousApplicationsMutateModeBusinessFacade {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimation;
    private final MiscellaneousAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    MiscellaneousApplicationsMutateModeBusinessFacade(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimation,
        MiscellaneousAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT scaleProperties(
        GraphName graphName,
        ScalePropertiesMutateConfig configuration,
        ResultBuilder<ScalePropertiesMutateConfig, ScalePropertiesResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ScalePropertiesMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            ScaleProperties,
            () -> estimation.scaleProperties(configuration),
            graph -> algorithms.scaleProperties(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
