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
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ScaleProperties;

public class MiscellaneousApplicationsStreamModeBusinessFacade {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimationFacade;
    private final MiscellaneousAlgorithms miscellaneousAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    MiscellaneousApplicationsStreamModeBusinessFacade(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimationFacade,
        MiscellaneousAlgorithms miscellaneousAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.miscellaneousAlgorithms = miscellaneousAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> Stream<RESULT> scaleProperties(
        GraphName graphName,
        ScalePropertiesStreamConfig configuration,
        StreamResultBuilder<ScalePropertiesResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ScaleProperties,
            () -> estimationFacade.scaleProperties(configuration),
            (graph, __) -> miscellaneousAlgorithms.scaleProperties(graph, configuration),
            resultBuilder
        );
    }
}
