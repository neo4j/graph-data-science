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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class BellmanFordResultBuilderForStreamMode implements StreamResultBuilder<BellmanFordResult, BellmanFordStreamResult> {
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final boolean routeRequested;
    private static final String COST_PROPERTY_NAME = "cost";
    private static final String RELATIONSHIP_TYPE_TEMPLATE = "PATH_%d";

    BellmanFordResultBuilderForStreamMode(
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        boolean routeRequested
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.routeRequested = routeRequested;
    }

    @Override
    public Stream<BellmanFordStreamResult> build(
        Graph graph,
        GraphStore graphStore,
        Optional<BellmanFordResult> result
    ) {
        if (result.isEmpty()) return Stream.of();

        var bellmanFordResult = result.get();

        // this is us handling the case of generated graphs and such
        var  pathFactoryFacade = PathFactoryFacade.create(routeRequested, nodeLookup,graphStore);

        var algorithmResult = getPathFindingResult(bellmanFordResult, bellmanFordResult.containsNegativeCycle());

        var resultStream = algorithmResult.mapPaths(route -> mapRoute(
            route,
            graph,
            bellmanFordResult.containsNegativeCycle(),
            pathFactoryFacade
        ));

        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    private BellmanFordStreamResult mapRoute(PathResult pathResult, IdMap idMap, boolean negativeCycle,PathFactoryFacade pathFactoryFacade){
        var nodeIds = pathResult.nodeIds();
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = idMap.toOriginalNodeId(nodeIds[i]);
        }
        var relationshipType = RelationshipType.withName(formatWithLocale(RELATIONSHIP_TYPE_TEMPLATE, pathResult.index()));

        double[] costs = pathResult.costs();

        var path =  pathFactoryFacade.createPath(
            nodeIds,
            costs,
            relationshipType,
            COST_PROPERTY_NAME
        );

        return new BellmanFordStreamResult(
            pathResult.index(),
            idMap.toOriginalNodeId(pathResult.sourceNode()),
            idMap.toOriginalNodeId(pathResult.targetNode()),
            pathResult.totalCost(),
            // 😿
            Arrays.stream(nodeIds).boxed().collect(Collectors.toCollection(() -> new ArrayList<>(nodeIds.length))),
            Arrays.stream(costs).boxed().collect(Collectors.toCollection(() -> new ArrayList<>(costs.length))),
            path,
            negativeCycle
        );
    }


    private static PathFindingResult getPathFindingResult(
        BellmanFordResult result,
        boolean containsNegativeCycle
    ) {
        if (containsNegativeCycle) return result.negativeCycles();

        return result.shortestPaths();
    }
}

