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
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This is the place where we capture all the algorithm-specific choices that piece together an algorithm as a _piece of work_.
 * It is the only place where you declare a validation requirements-algorithm method-estimation method-label combo,
 * because anything else would be duplication.
 * Side effects and result rendering behaviours get injected as parameters.
 */
public class CentralityAlgorithmsBusinessFacade {
    private final InstrumentedCentralityAlgorithms algorithms;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    public CentralityAlgorithmsBusinessFacade(
        InstrumentedCentralityAlgorithms algorithms,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> articulationPoints(
        GraphName graphName,
        ArticulationPointsBaseConfig configuration,
        Optional<SideEffect<ArticulationPointsResult, METADATA>> sideEffect,
        ResultRenderer<ArticulationPointsResult, RESULT, METADATA> resultRenderer,
        boolean shouldComputeComponents
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            new UndirectedOnlyRequirement("Articulation Points"),
            graph -> algorithms.articulationPoints(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.articulationPoints(shouldComputeComponents),
            AlgorithmLabel.ArticulationPoints,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityBaseConfig configuration,
        Optional<SideEffect<HarmonicResult, METADATA>> sideEffect,
        ResultRenderer<HarmonicResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            new UndirectedOnlyRequirement("Articulation Points"),
            graph -> algorithms.harmonicCentrality(graph, configuration),
            estimationFacade::harmonicCentrality,
            AlgorithmLabel.ArticulationPoints,
            sideEffect,
            resultRenderer
        );
    }
}
