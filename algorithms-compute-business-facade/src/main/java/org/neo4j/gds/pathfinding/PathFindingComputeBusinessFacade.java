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
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.AlgorithmGraphStoreRequirementsBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.NoAlgorithmRequirements;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAllLabels;
import org.neo4j.gds.core.loading.validation.NodePropertyTypeRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodeRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodeRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodesGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowParameters;
import org.neo4j.gds.pathfinding.validation.RandomWalkGraphValidation;
import org.neo4j.gds.paths.astar.AStarParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSourceTargetParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.yens.YensParameters;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.List;
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.allShortestPaths(
            graph,
            parameters,
            jobId
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> bellmanFord(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        BellmanFordParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<BellmanFordResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.bellmanFord(
            graph,
            parameters,
            jobId,
            logProgress
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            )),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.breadthFirstSearch(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> deltaStepping(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        DeltaSteppingParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeRequirement(parameters.sourceNode())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.deltaStepping(
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            )),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.depthFirstSearch(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> kSpanningTree(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        KSpanningTreeParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<SpanningTree>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(new SourceNodeRequirement(parameters.sourceNode()))
                .withAlgorithmRequirement(new UndirectedOnlyRequirement("K-Spanning Tree"))
                .build(),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.kSpanningTree(
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
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
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
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.maxFlow(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodesRequirement(parameters.sourceNodes())),
            Optional.of(new RandomWalkGraphValidation(parameters.concurrency(), executorService)),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.randomWalk(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodesRequirement(parameters.sourceNodes())),
            Optional.of(new RandomWalkGraphValidation(parameters.concurrency(), executorService)),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.randomWalkCountingNodeVisits(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> pcst(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        PCSTParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PrizeSteinerTreeResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
             new AlgorithmGraphStoreRequirementsBuilder()
                 .withAlgorithmRequirement(new UndirectedOnlyRequirement("Prize-collecting Steiner Tree"))
                 .withAlgorithmRequirement(new NodePropertyMustExistOnAllLabels(parameters.prizeProperty()))
                 .withAlgorithmRequirement(new NodePropertyTypeRequirement(parameters.prizeProperty(), List.of(ValueType.DOUBLE)))
                 .build(),
            Optional.empty(),
            user,
            databaseId
        );

        var graph = graphResources.graph();

        return computeFacade.pcst(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> singlePairShortestPathAStar(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        AStarParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeTargetNodeRequirement(parameters.sourceNode(), parameters.targetNode())),
            Optional.of(new RandomWalkGraphValidation(parameters.concurrency(), executorService)),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.singlePairShortestPathAStar(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> singlePairShortestPathDijkstra(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        DijkstraSourceTargetParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(parameters.sourceNode(), parameters.targetsList())),
            Optional.of(new RandomWalkGraphValidation(parameters.concurrency(), executorService)),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.singlePairShortestPathDijkstra(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> singlePairShortestPathYens(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        YensParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeTargetNodeRequirement(parameters.sourceNode(), parameters.targetNode())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.singlePairShortestPathYens(
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeRequirement(parameters.sourceNode())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.singleSourceShortestPathDijkstra(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> spanningTree(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        SpanningTreeParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<SpanningTree>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(new SourceNodeRequirement(parameters.sourceNode()))
                .withAlgorithmRequirement(new UndirectedOnlyRequirement("Spanning Tree"))
                .build(),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.spanningTree(
            graph,
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> steinerTree(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        SteinerTreeParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<SteinerTreeResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new SourceNodeTargetNodesGraphStoreValidation(parameters.sourceNode(), parameters.targetNodes())),
            Optional.empty(),
            user,
            databaseId
        );
        var graph = graphResources.graph();

        return computeFacade.steinerTree(
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
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            Optional.empty(),
            user,
            databaseId
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
