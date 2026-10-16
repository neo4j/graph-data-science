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

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class PathFindingResultBuilderForStreamMode implements StreamResultBuilder<PathFindingResult, PathFindingStreamResult> {
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final boolean pathRequested;

    PathFindingResultBuilderForStreamMode(
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        boolean pathRequested
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.pathRequested = pathRequested;
    }

    @Override
    public Stream<PathFindingStreamResult> build(
        NodeTranslator nodeTranslator,
        GraphStore graphStore,
        Optional<PathFindingResult> result
    ) {
        if (result.isEmpty()) return Stream.of();

        var pathFindingResult = result.get();
        var pathFactoryFacade=  PathFactoryFacade.create(pathRequested, nodeLookup,
            graphStore.capabilities().canWriteToLocalDatabase()
        );

        var resultStream = pathFindingResult.mapPaths(pathResult -> mapPath(pathResult, nodeTranslator,pathFactoryFacade));

        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    PathFindingStreamResult mapPath(PathResult pathResult, NodeTranslator nodeTranslator, PathFactoryFacade pathFactoryFacade){
        var nodeIds = pathResult.nodeIds();
        var costs = pathResult.costs();
        var pathIndex = pathResult.index();

        var relationshipType = RelationshipType.withName(formatWithLocale("PATH_%d", pathIndex));

        // convert internal ids to Neo ids
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = nodeTranslator.toOriginalNodeId(nodeIds[i]);
        }
        var path = pathFactoryFacade.createPath(
            nodeIds,
            costs,
            relationshipType,
            PathFindingStreamResult.COST_PROPERTY_NAME
        );

        return new PathFindingStreamResult(
            pathIndex,
            nodeTranslator.toOriginalNodeId(pathResult.sourceNode()),
            nodeTranslator.toOriginalNodeId(pathResult.targetNode()),
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
    }
}
