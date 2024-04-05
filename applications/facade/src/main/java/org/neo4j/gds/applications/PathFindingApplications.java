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
package org.neo4j.gds.applications;

import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithms;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
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
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        RequestScopedDependencies requestScopedDependencies,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        AlgorithmEstimationTemplate algorithmEstimationTemplate
    ) {
        var pathFindingAlgorithms = new PathFindingAlgorithms(
            log,
            requestScopedDependencies,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        var estimationModeFacade = new PathFindingAlgorithmsEstimationModeBusinessFacade(algorithmEstimationTemplate);

        var mutateModeFacade = new PathFindingAlgorithmsMutateModeBusinessFacade(
            estimationModeFacade,
            pathFindingAlgorithms,
            algorithmProcessingTemplate
        );

        var statsModeFacade = new PathFindingAlgorithmsStatsModeBusinessFacade(
            algorithmProcessingTemplate,
            estimationModeFacade,
            pathFindingAlgorithms
        );

        var streamModeFacade = new PathFindingAlgorithmsStreamModeBusinessFacade(
            algorithmProcessingTemplate,
            estimationModeFacade,
            pathFindingAlgorithms
        );

        var writeModeFacade = new PathFindingAlgorithmsWriteModeBusinessFacade(
            log,
            algorithmProcessingTemplate,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            requestScopedDependencies,
            taskRegistryFactory,
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
