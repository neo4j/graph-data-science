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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.PathFactory.create;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class PathFindingResultBuilderForStreamMode extends ResultBuilder<PathFindingResult, Stream<PathFindingStreamResult>> {
    private final NodeLookup nodeLookup;
    private final boolean pathRequested;

    PathFindingResultBuilderForStreamMode(NodeLookup nodeLookup, boolean pathRequested) {
        this.nodeLookup = nodeLookup;
        this.pathRequested = pathRequested;
    }

    @Override
    public Stream<PathFindingStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<PathFindingResult> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return Stream.of();

        // this is us handling the case of generated graphs and such
        var createCypherPaths = pathRequested && graphStore.capabilities().canWriteToLocalDatabase();

        return result.get().mapPaths(pathResult -> {
            var nodeIds = pathResult.nodeIds();
            var costs = pathResult.costs();
            var pathIndex = pathResult.index();

            var relationshipType = RelationshipType.withName(formatWithLocale("PATH_%d", pathIndex));

            // convert internal ids to Neo ids
            for (int i = 0; i < nodeIds.length; i++) {
                nodeIds[i] = graph.toOriginalNodeId(nodeIds[i]);
            }

            Path path = null;
            if (createCypherPaths) {
                path = create(
                    nodeLookup,
                    nodeIds,
                    costs,
                    relationshipType,
                    PathFindingStreamResult.COST_PROPERTY_NAME
                );
            }

            return new PathFindingStreamResult(
                pathIndex,
                graph.toOriginalNodeId(pathResult.sourceNode()),
                graph.toOriginalNodeId(pathResult.targetNode()),
                pathResult.totalCost(),
                // 😿
                Arrays.stream(nodeIds)
                    .boxed()
                    .collect(Collectors.toCollection(() -> new ArrayList<>(nodeIds.length))),
                Arrays.stream(costs)
                    .boxed()
                    .collect(Collectors.toCollection(() -> new ArrayList<>(costs.length))),
                path
            );
        });
    }
}
