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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFactoryFacade;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class BellmanFordStreamResultTransformer implements ResultTransformer<TimedAlgorithmResult<BellmanFordResult>, Stream<BellmanFordStreamResult>> {

    private static final String COST_PROPERTY_NAME = "cost";
    private static final String RELATIONSHIP_TYPE_TEMPLATE = "PATH_%d";

    private final Graph graph;
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final PathFactoryFacade pathFactoryFacade;

    BellmanFordStreamResultTransformer(
        Graph graph,
        CloseableResourceRegistry closeableResourceRegistry,
        PathFactoryFacade pathFactoryFacade
    ) {
        this.graph = graph;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.pathFactoryFacade = pathFactoryFacade;
    }

    public Stream<BellmanFordStreamResult> apply(TimedAlgorithmResult<BellmanFordResult> timedAlgorithmResult) {
        var bellmanFordResult = timedAlgorithmResult.result();
        var containsNegativeCycle = bellmanFordResult.containsNegativeCycle();
        var paths = containsNegativeCycle
            ? bellmanFordResult.negativeCycles()
            : bellmanFordResult.shortestPaths();

        var resultStream = paths.mapPaths(route -> mapRoute(
            route,
            graph,
            containsNegativeCycle,
            pathFactoryFacade
        ));

        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    private BellmanFordStreamResult mapRoute(
        PathResult pathResult,
        IdMap idMap,
        boolean negativeCycle,
        PathFactoryFacade pathFactoryFacade
    ) {
        var relationshipType = RelationshipType.withName(formatWithLocale(
            RELATIONSHIP_TYPE_TEMPLATE,
            pathResult.index()
        ));

        var nodeIds = Arrays.stream(pathResult.nodeIds())
            .map(idMap::toOriginalNodeId)
            .boxed()
            .toList();

        var costs = Arrays.stream(pathResult.costs())
            .boxed()
            .toList();

        var path = pathFactoryFacade.createPath(
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
            nodeIds,
            costs,
            path,
            negativeCycle
        );
    }
}
