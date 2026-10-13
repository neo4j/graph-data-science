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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.indexInverse.InverseRelationshipsConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.undirected.ToUndirectedConfig;
import org.neo4j.gds.walking.CollapsePathConfig;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

final class InstrumentedMiscellaneousAlgorithms {
    private final MiscellaneousApplicationsEstimationModeBusinessFacade estimations;
    private final TrackedMiscellaneousAlgorithms trackedAlgorithms;
    private final LaunchConvenience launchConvenience;

    private InstrumentedMiscellaneousAlgorithms(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimations,
        TrackedMiscellaneousAlgorithms trackedAlgorithms,
        LaunchConvenience launchConvenience
    ) {
        this.estimations = estimations;
        this.trackedAlgorithms = trackedAlgorithms;
        this.launchConvenience = launchConvenience;
    }

    static InstrumentedMiscellaneousAlgorithms create(
        MiscellaneousApplicationsEstimationModeBusinessFacade estimation,
        LaunchConvenience launchConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag
    ) {
        var algorithms = new MiscellaneousAlgorithms(terminationFlag);

        var trackedAlgorithms = new TrackedMiscellaneousAlgorithms(algorithms, progressTrackerCreator);

        return new InstrumentedMiscellaneousAlgorithms(estimation, trackedAlgorithms, launchConvenience);
    }

    <RESULT, METADATA> CompletableFuture<RESULT> collapsePath(
        GraphName graphName,
        CollapsePathConfig configuration,
        Optional<SideEffect<SingleTypeRelationships, METADATA>> sideEffect,
        ResultRenderer<SingleTypeRelationships, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (__, ___, graphStore) -> trackedAlgorithms.collapsePath(graphStore, configuration),
            estimations::collapsePath,
            AlgorithmLabel.CollapsePath,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> indexInverse(
        GraphName graphName,
        InverseRelationshipsConfig configuration,
        Optional<SideEffect<Map<RelationshipType, SingleTypeRelationships>, METADATA>> sideEffect,
        ResultRenderer<Map<RelationshipType, SingleTypeRelationships>, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, graphStore) -> trackedAlgorithms.indexInverse(graph, graphStore, configuration),
            () -> estimations.indexInverse(configuration),
            AlgorithmLabel.IndexInverse,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> scaleProperties(
        GraphName graphName,
        ScalePropertiesBaseConfig configuration,
        Optional<SideEffect<ScalePropertiesResult, METADATA>> sideEffect,
        ResultRenderer<ScalePropertiesResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> trackedAlgorithms.scaleProperties(graph, configuration),
            () -> estimations.scaleProperties(configuration),
            AlgorithmLabel.ScaleProperties,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> toUndirected(
        GraphName graphName,
        ToUndirectedConfig configuration,
        Optional<SideEffect<SingleTypeRelationships, METADATA>> sideEffect,
        ResultRenderer<SingleTypeRelationships, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (__, ___, graphStore) -> trackedAlgorithms.toUndirected(graphStore, configuration),
            () -> estimations.toUndirected(configuration),
            AlgorithmLabel.ToUndirected,
            sideEffect,
            resultRenderer
        );
    }
}
