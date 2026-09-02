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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;

import java.util.concurrent.CompletableFuture;

/**
 * This is the place where we capture all the algorithm-specific choices that piece together an algorithm as a _piece of work_.
 * It is the only place where you declare a validation requirements-algorithm method-estimation method-label combo,
 * because anything else would be duplication.
 * Side effects and result rendering behaviours get injected as parameters.
 */
public class CentralityAlgorithmsBusinessFacade {
    private final CentralityBusinessAlgorithms centralityAlgorithms;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    CentralityAlgorithmsBusinessFacade(
        CentralityBusinessAlgorithms centralityAlgorithms,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.centralityAlgorithms = centralityAlgorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    /**
     * @deprecated that Void in the result renderer will change once we tunnel mutate and write through here.
     */
    @Deprecated
    public <RESULT> CompletableFuture<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        ResultRenderer<ArticulationPointsResult, RESULT, Void> resultRenderer,
        boolean shouldComputeComponents
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            new UndirectedOnlyRequirement("Articulation Points"),
            graph -> centralityAlgorithms.articulationPoints(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.articulationPoints(shouldComputeComponents),
            AlgorithmLabel.ArticulationPoints,
            resultRenderer
        );
    }
}
