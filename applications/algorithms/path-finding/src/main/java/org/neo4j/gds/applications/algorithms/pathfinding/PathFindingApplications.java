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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.logging.Log;

/**
 * The facade over path finding applications
 */
public final class PathFindingApplications {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade;
    private final PathFindingAlgorithmsMutateModeBusinessFacade mutateModeFacade;
    private final PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade;
    private final PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade;
    private final PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade;

    private PathFindingApplications(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade,
        PathFindingAlgorithmsMutateModeBusinessFacade mutateModeFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade
    ) {
        this.estimationModeFacade = estimationModeFacade;
        this.mutateModeFacade = mutateModeFacade;
        this.statsModeFacade = statsModeFacade;
        this.streamModeFacade = streamModeFacade;
        this.writeModeFacade = writeModeFacade;
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
        ProgressTrackerCreator progressTrackerCreator,
        MutateNodeProperty mutateNodeProperty
    ) {
        var pathFindingAlgorithms = new PathFindingAlgorithms(requestScopedDependencies, progressTrackerCreator);

        var estimationModeFacade = new PathFindingAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);

        var mutateModeFacade = new PathFindingAlgorithmsMutateModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplateConvenience,
            mutateNodeProperty
        );

        var statsModeFacade = new PathFindingAlgorithmsStatsModeBusinessFacade(
            algorithmProcessingTemplateConvenience,
            estimationModeFacade,
            pathFindingAlgorithms
        );

        var streamModeFacade = new PathFindingAlgorithmsStreamModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplateConvenience
        );

        var writeModeFacade = new PathFindingAlgorithmsWriteModeBusinessFacade(
            log,
            algorithmProcessingTemplateConvenience,
            requestScopedDependencies,
            writeContext,
            estimationModeFacade,
            pathFindingAlgorithms
        );

        return new PathFindingApplications(
            estimationModeFacade,
            mutateModeFacade,
            statsModeFacade,
            streamModeFacade,
            writeModeFacade
        );
    }

    public PathFindingAlgorithmsEstimationModeBusinessFacade estimate() {
        return estimationModeFacade;
    }

    public PathFindingAlgorithmsMutateModeBusinessFacade mutate() {
        return mutateModeFacade;
    }

    public PathFindingAlgorithmsStatsModeBusinessFacade stats() {
        return statsModeFacade;
    }

    public PathFindingAlgorithmsStreamModeBusinessFacade stream() {
        return streamModeFacade;
    }

    public PathFindingAlgorithmsWriteModeBusinessFacade write() {
        return writeModeFacade;
    }
}
