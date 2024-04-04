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
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStreamModeBusinessFacade;

/**
 * The facade over path finding applications
 */
public final class PathFindingApplications {
    private final PathFindingAlgorithmsStreamModeBusinessFacade pathFindingAlgorithmsStreamModeBusinessFacade;

    private PathFindingApplications(PathFindingAlgorithmsStreamModeBusinessFacade pathFindingAlgorithmsStreamModeBusinessFacade) {
        this.pathFindingAlgorithmsStreamModeBusinessFacade = pathFindingAlgorithmsStreamModeBusinessFacade;
    }

    /**
     * Here we hide dull and boring structure
     */
    public static PathFindingApplications create(
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        PathFindingAlgorithms pathFindingAlgorithms,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade
    ) {
        var streamModeFacade = new PathFindingAlgorithmsStreamModeBusinessFacade(
            algorithmProcessingTemplate,
            estimationFacade,
            pathFindingAlgorithms
        );

        return new PathFindingApplications(streamModeFacade);
    }

    public PathFindingAlgorithmsStreamModeBusinessFacade stream() {
        return pathFindingAlgorithmsStreamModeBusinessFacade;
    }
}
