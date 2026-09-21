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
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesStreamConfig;
import org.neo4j.gds.core.loading.validation.DirectedOnlyRequirement;
import org.neo4j.gds.core.loading.validation.PregelPropertiesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.core.loading.validation.ValidationRule;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This is the place where we capture all the algorithm-specific choices that piece together an algorithm as a _piece of work_.
 * It is the only place where you declare a validation requirements-algorithm method-estimation method-label combo,
 * because anything else would be duplication.
 * Side effects and result rendering behaviours get injected as parameters.
 */
public final class InstrumentedCentralityAlgorithms {
    private final TrackedCentralityAlgorithms algorithms;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;
    private final HitsHookGenerator hitsHookGenerator;

    private InstrumentedCentralityAlgorithms(
        TrackedCentralityAlgorithms algorithms,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience,
        HitsHookGenerator hitsHookGenerator
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
        this.hitsHookGenerator = hitsHookGenerator;
    }

    public static InstrumentedCentralityAlgorithms create(
        TrackedCentralityAlgorithms algorithms,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag
    ) {
        var hitsHookGenerator = new HitsHookGenerator(progressTrackerCreator, terminationFlag);

        return new InstrumentedCentralityAlgorithms(
            algorithms,
            estimationFacade,
            launchConvenience,
            hitsHookGenerator
        );
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
            Optional.empty(),
            Set.of(new UndirectedOnlyRequirement("Articulation Points")),
            Optional.empty(),
            Optional.empty(),
            (graph, __) -> algorithms.articulationPoints(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.articulationPoints(shouldComputeComponents),
            AlgorithmLabel.ArticulationPoints,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> bridges(
        GraphName graphName,
        BridgesStreamConfig configuration,
        Optional<SideEffect<BridgeResult, METADATA>> sideEffect,
        ResultRenderer<BridgeResult, RESULT, METADATA> resultRenderer,
        boolean shouldComputeComponents
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Set.of(new UndirectedOnlyRequirement("Bridges")),
            Optional.empty(),
            Optional.empty(),
            (graph, __) -> algorithms.bridges(graph, configuration, shouldComputeComponents),
            () -> estimationFacade.bridges(shouldComputeComponents),
            AlgorithmLabel.Bridges,
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
            Optional.empty(),
            Set.of(ValidationRule.EMPTY),
            Optional.empty(),
            Optional.empty(),
            (graph, __) -> algorithms.harmonicCentrality(graph, configuration),
            estimationFacade::harmonicCentrality,
            AlgorithmLabel.HarmonicCentrality,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> hits(
        GraphName graphName,
        HitsConfig configuration,
        Optional<SideEffect<PregelResult, METADATA>> sideEffect,
        ResultRenderer<PregelResult, RESULT, METADATA> resultRenderer
    ) {
        // the hook creates inverse indexes
        var etlHook = hitsHookGenerator.createETLHook(configuration);

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            configuration.relationshipWeightProperty(),
            Set.of(
                new PregelPropertiesRequirement(configuration.writeProperty()),
                new DirectedOnlyRequirement("Hits")
            ),
            Optional.of(Set.of(etlHook)),
            Optional.empty(),
            (graph, __) -> algorithms.hits(graph, configuration),
            estimationFacade::hits,
            AlgorithmLabel.HITS,
            sideEffect,
            resultRenderer
        );
    }
}
