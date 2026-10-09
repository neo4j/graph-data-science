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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.applications.algorithms.pathfinding.RandomWalkGraphValidation;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.traversal.RandomWalkParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class PathFindingComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final PathFindingComputeFacade computeFacade;
    private final ExecutorService executorService;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final User user;
    private final DatabaseId databaseId;

    public PathFindingComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        PathFindingComputeFacade computeFacade,
        ExecutorService executorService,
        User user,
        DatabaseId databaseId
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
        this.executorService = executorService;
        this.user = user;
        this.databaseId = databaseId;
    }

    public <TR> CompletableFuture<TR> randomWalkCountingNodeVisits(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        RandomWalkParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HugeAtomicLongArray>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodesRequirement(parameters.sourceNodes())),
            true,
            Optional.of(new RandomWalkGraphValidation(parameters.concurrency(), executorService))
        );
        var graph = graphResources.graph();

        return computeFacade.randomWalkCountingNodeVisits(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> singleSourceShortestPathDijkstra(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        DijkstraSingleSourceParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeRequirement(parameters.sourceNode())),
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.singleSourceShortestPathDijkstra(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }
}
