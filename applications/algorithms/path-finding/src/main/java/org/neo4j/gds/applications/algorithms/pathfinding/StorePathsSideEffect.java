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

import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * This is our hook for storing Dijkstra paths in a place where they can later be found.
 * I.e. we stick them in a map, keyed by relationship type as a pseudo parameter.
 * The paths themselves are represented as internal node ids, costs of the hops between them, and the sum of the costs.
 * It is the responsibility of outer layers to map the internal node ids back into user space.
 */
class StorePathsSideEffect implements SideEffect<PathFindingResult, Void> {
    private final Map<String, Stream<PathUsingInternalNodeIds>> paths;
    private final String relationshipTypeAsString;

    StorePathsSideEffect(
        Map<String, Stream<PathUsingInternalNodeIds>> paths,
        String relationshipTypeAsString
    ) {
        this.paths = paths;
        this.relationshipTypeAsString = relationshipTypeAsString;
    }

    @Override
    public Optional<Void> process(GraphResources graphResources, Optional<PathFindingResult> pathFindingResult) {
        if (pathFindingResult.isEmpty()) {
            return Optional.empty();
        }

        var actualPathResult = pathFindingResult.get();

        // just record the paths, no conversions, that happens later
        var pathStream = actualPathResult.mapPaths(
            pathResult -> new PathUsingInternalNodeIds(
                pathResult.sourceNode(),
                pathResult.targetNode(),
                pathResult.nodeIds(),
                pathResult.costs(),
                pathResult.totalCost()
            )
        );

        paths.put(relationshipTypeAsString, pathStream);

        // we have no interesting metadata
        return Optional.empty();
    }
}
