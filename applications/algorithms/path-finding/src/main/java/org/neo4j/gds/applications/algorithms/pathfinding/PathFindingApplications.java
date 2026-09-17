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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.logging.Log;

/**
 * The facade over path finding applications
 */
public final class PathFindingApplications {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimation;
    private final PathFindingAlgorithmsMutateModeBusinessFacade mutate;
    private final InstrumentedPathFindingAlgorithms raw;
    private final PathFindingAlgorithmsStatsModeBusinessFacade stats;
    private final PathFindingAlgorithmsStreamModeBusinessFacade stream;
    private final PathFindingAlgorithmsWriteModeBusinessFacade write;

    private PathFindingApplications(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimation,
        PathFindingAlgorithmsMutateModeBusinessFacade mutate,
        InstrumentedPathFindingAlgorithms raw,
        PathFindingAlgorithmsStatsModeBusinessFacade stats,
        PathFindingAlgorithmsStreamModeBusinessFacade stream,
        PathFindingAlgorithmsWriteModeBusinessFacade write
    ) {
        this.estimation = estimation;
        this.mutate = mutate;
        this.raw = raw;
        this.stats = stats;
        this.stream = stream;
        this.write = write;
    }

    /**
     * Here we hide dull and boring structure
     */
    public static PathFindingApplications create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodePropertyService mutateNodeProperty,
        MutateRelationshipService mutateRelationshipService,
        LaunchConvenience launchConvenience,
        Synchroniser synchroniser
    ) {
        var algorithms = new PathFindingAlgorithms(log);
        var pathFindingAlgorithms = new PathFindingAlgorithmsBusinessFacade(algorithms, requestScopedDependencies, progressTrackerCreator);

        var estimationModeFacade = new PathFindingAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);
        var writeRelationshipService = new WriteRelationshipService(log, requestScopedDependencies, writeContext);

        var trackedAlgorithms = new TrackedPathFindingAlgorithms(
            algorithms,
            requestScopedDependencies,
            progressTrackerCreator
        );
        var raw = new InstrumentedPathFindingAlgorithms(trackedAlgorithms, estimationModeFacade, launchConvenience);

        var mutateModeFacade = new PathFindingAlgorithmsMutateModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplateConvenience,
            algorithmProcessingTemplate,
            mutateNodeProperty,
            mutateRelationshipService,
            raw,
            synchroniser
        );

        var statsModeFacade = new PathFindingAlgorithmsStatsModeBusinessFacade(
            algorithmProcessingTemplateConvenience,
            estimationModeFacade,
            pathFindingAlgorithms,
            raw,
            synchroniser
        );

        var streamModeFacade = new PathFindingAlgorithmsStreamModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplateConvenience,
            raw,
            synchroniser
        );

        var writeModeFacade = new PathFindingAlgorithmsWriteModeBusinessFacade(
            log,
            algorithmProcessingTemplateConvenience,
            requestScopedDependencies,
            writeContext,
            writeRelationshipService,
            estimationModeFacade,
            pathFindingAlgorithms,
            raw,
            synchroniser
        );

        return new PathFindingApplications(
            estimationModeFacade,
            mutateModeFacade,
            raw,
            statsModeFacade,
            streamModeFacade,
            writeModeFacade
        );
    }

    public PathFindingAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimation;
    }

    public PathFindingAlgorithmsMutateModeBusinessFacade mutate() {
        return mutate;
    }

    public InstrumentedPathFindingAlgorithms raw() {
        return raw;
    }

    public PathFindingAlgorithmsStatsModeBusinessFacade stats() {
        return stats;
    }

    public PathFindingAlgorithmsStreamModeBusinessFacade stream() {
        return stream;
    }

    public PathFindingAlgorithmsWriteModeBusinessFacade write() {
        return write;
    }
}
