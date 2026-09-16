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
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.conductance.ConductanceBaseConfig;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAnyLabel;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleResult;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Community algorithms in all modes need to go through here, lest we have duplication.
 */
public class InstrumentedCommunityAlgorithms {
    private final TrackedCommunityAlgorithms algorithms;
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    InstrumentedCommunityAlgorithms(
        TrackedCommunityAlgorithms algorithms,
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> conductance(
        GraphName graphName,
        ConductanceBaseConfig configuration,
        Optional<SideEffect<ConductanceResult, METADATA>> sideEffect,
        ResultRenderer<ConductanceResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            configuration.relationshipWeightProperty(),
            Set.of(new NodePropertyMustExistOnAnyLabel(configuration.communityProperty())),
            Optional.empty(),
            (graph, __) -> algorithms.conductance(graph, configuration),
            estimationFacade::conductance,
            AlgorithmLabel.Conductance,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> modularity(
        GraphName graphName,
        ModularityBaseConfig configuration,
        Optional<SideEffect<ModularityResult, METADATA>> sideEffect,
        ResultRenderer<ModularityResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            configuration.relationshipWeightProperty(),
            Set.of(new NodePropertyMustExistOnAnyLabel(configuration.communityProperty())),
            Optional.empty(),
            (graph, __) -> algorithms.modularity(graph, configuration),
            estimationFacade::modularity,
            AlgorithmLabel.Modularity,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> triangles(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        Optional<SideEffect<Stream<TriangleResult>, METADATA>> sideEffect,
        ResultRenderer<Stream<TriangleResult>, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Set.of(TriangleCountGraphStoreRequirements.create(configuration.labelFilter())),
            Optional.empty(),
            (graph, __) -> algorithms.triangles(graph, configuration),
            estimationFacade::triangles,
            AlgorithmLabel.Triangles,
            sideEffect,
            resultRenderer
        );
    }
}
