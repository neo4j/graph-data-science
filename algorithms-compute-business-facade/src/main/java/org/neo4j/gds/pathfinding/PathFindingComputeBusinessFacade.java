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
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.RelationshipPropertyGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodesGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowParameters;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFParameters;
import org.neo4j.gds.pathfinding.validation.FlowAlgorithmRequirements;
import org.neo4j.gds.pathfinding.validation.RandomWalkGraphValidation;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

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

    public <TR> CompletableFuture<TR> allShortestPaths(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        AllShortestPathsParameters parameters,
        JobId jobId,
        ResultTransformerBuilder<TimedAlgorithmResult<Stream<AllShortestPathsStreamResult>>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            GraphStoreValidation.DISABLED,
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.allShortestPaths(
            graph,
            parameters,
            jobId
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> breadthFirstSearch(
        GraphName graphName,
        GraphParameters graphParameters,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HugeLongArray>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            )),
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.breadthFirstSearch(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> depthFirstSearch(
        GraphName graphName,
        GraphParameters graphParameters,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HugeLongArray>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            )),
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.depthFirstSearch(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> longestPath(
        GraphName graphName,
        GraphParameters graphParameters,
        DagLongestPathParameters parameters,
        JobId jobId,
        Optional<String> relationshipProperty,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            GraphStoreValidation.DISABLED,
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.longestPath(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> maxFlow(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        MaxFlowParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<FlowResult>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(FlowAlgorithmRequirements.create(parameters)),
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.maxFlow(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> mcmf(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> capacityProperty,
        Optional<String> costProperty,
        MCMFParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<CostFlowResult>, TR> resultTransformerBuilder
    ) {
        var capacityGraphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            capacityProperty,
            new GraphStoreValidation(FlowAlgorithmRequirements.create(parameters.maxFlowParameters())),
            true,
            Optional.empty()
        );

        //the below validation just renames the outputed error variable name (because its not relationship weight property)
        var costGraphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            costProperty,
            new GraphStoreValidation(new RelationshipPropertyGraphStoreValidation(costProperty, "costProperty")),
            true,
            Optional.empty()
        );

        var capacityGraph = capacityGraphResources.graph();
        var costGraph = costGraphResources.graph();

        return computeFacade.mcmf(
            capacityGraph,
            costGraph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(capacityGraphResources));
    }


    public <TR> CompletableFuture<TR> randomWalk(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        RandomWalkParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<Stream<long[]>>, TR> resultTransformerBuilder
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

        return computeFacade.randomWalk(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> topologicalSort(
        GraphName graphName,
        GraphParameters graphParameters,
        TopologicalSortParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<TopologicalSortResult>, TR> resultTransformerBuilder
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            Optional.empty(),
            GraphStoreValidation.DISABLED,
            true,
            Optional.empty()
        );
        var graph = graphResources.graph();

        return computeFacade.topologicalSort(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }
}
