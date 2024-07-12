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
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesWriteConfig;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ScaleProperties;

public class MiscellaneousApplicationsWriteModeBusinessFacade {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimationFacade;
    private final MiscellaneousAlgorithms miscellaneousAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final WriteToDatabase writeToDatabase;

    MiscellaneousApplicationsWriteModeBusinessFacade(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimationFacade,
        MiscellaneousAlgorithms miscellaneousAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        WriteToDatabase writeToDatabase
    ) {
        this.estimationFacade = estimationFacade;
        this.miscellaneousAlgorithms = miscellaneousAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.writeToDatabase = writeToDatabase;
    }

    public <RESULT> RESULT scaleProperties(
        GraphName graphName,
        ScalePropertiesWriteConfig configuration,
        ResultBuilder<ScalePropertiesWriteConfig, ScalePropertiesResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ScalePropertiesWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            ScaleProperties,
            () -> estimationFacade.scaleProperties(configuration),
            (graph, __) -> miscellaneousAlgorithms.scaleProperties(graph, configuration),
            writeStep,
            resultBuilder
        );
    }
}
