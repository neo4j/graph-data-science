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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.WCC;

public class CommunityAlgorithmsMutateModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplate template;
    private final MutateNodeProperty mutateNodeProperty;

    public CommunityAlgorithmsMutateModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplate template,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.template = template;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccMutateConfig configuration,
        ResultBuilder<WccMutateConfig, DisjointSetStruct, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new WccMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            WCC,
            () -> estimation.wcc(configuration),
            graph -> algorithms.wcc(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

}
