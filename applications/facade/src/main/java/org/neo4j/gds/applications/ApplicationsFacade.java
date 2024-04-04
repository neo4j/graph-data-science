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

import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithms;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

/**
 * This is the top level facade for GDS applications. If you are integrating GDS,
 * this is the one thing you want to work with. See for example Neo4j Procedures.
 * <p>
 * We use the facade pattern for well known reasons,
 * and we apply a breakdown into sub-facades to keep things smaller and more manageable.
 */
public final class ApplicationsFacade {
    private final PathFindingApplications pathFindingApplications;

    private ApplicationsFacade(PathFindingApplications pathFindingApplications) {
        this.pathFindingApplications = pathFindingApplications;
    }

    /**
     * We can stuff all the boring structure stuff in here so nobody needs to worry about it.
     */
    public static ApplicationsFacade create(
        Log log,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        PathFindingAlgorithms pathFindingAlgorithms,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade
    ) {
        var pathFindingApplications = PathFindingApplications.create(
            log,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            algorithmProcessingTemplate,
            pathFindingAlgorithms,
            estimationFacade
        );

        return new ApplicationsFacade(pathFindingApplications);
    }

    public PathFindingApplications pathFinding() {
        return pathFindingApplications;
    }
}
