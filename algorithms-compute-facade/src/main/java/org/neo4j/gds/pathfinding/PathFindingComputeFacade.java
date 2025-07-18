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
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.pathfinding.MSBFSASPAlgorithmFactory;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.NoAlgorithmValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodesGraphStoreValidation;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.kspanningtree.KSpanningTreeTask;
import org.neo4j.gds.pathfinding.validation.KSpanningTreeGraphStoreValidation;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BFS;
import org.neo4j.gds.paths.traverse.BFSProgressTask;
import org.neo4j.gds.paths.traverse.DFS;
import org.neo4j.gds.paths.traverse.DFSProgressTask;
import org.neo4j.gds.paths.traverse.ExitAndAggregation;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class PathFindingComputeFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;
    // This service is what the algorithms use for parallelism.
    private final ExecutorService executorService;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final User user;
    private final DatabaseId databaseId;
    private final TerminationFlag terminationFlag;

    public PathFindingComputeFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        AsyncAlgorithmCaller algorithmCaller,
        User user,
        DatabaseId databaseId,
        ExecutorService executorService,
        TerminationFlag terminationFlag,
        ProgressTrackerFactory progressTrackerFactory
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.algorithmCaller = algorithmCaller;
        this.user = user;
        this.databaseId = databaseId;
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.progressTrackerFactory = progressTrackerFactory;
    }

    CompletableFuture<Stream<AllShortestPathsStreamResult>> allShortestPaths(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        AllShortestPathsParameters parameters,
        JobId jobId
    ) {
        // Create ProgressTracker
        // `allShortestPaths` doesn't use progress tracker (yet 🤔)
        var progressTracker = progressTrackerFactory.nullTracker();

        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new NoAlgorithmValidation(),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create the algorithm
        var allShortestPaths = MSBFSASPAlgorithmFactory.create(
            graph,
            parameters,
            executorService,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            allShortestPaths::compute,
            jobId
        );

    }

    CompletableFuture<BellmanFordResult> bellmanFord(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        BellmanFordParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            BellmanFordProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new NoAlgorithmValidation(),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create the algorithm
        var bellmanFord = new BellmanFord(
            graph,
            progressTracker,
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.trackNegativeCycles(),
            parameters.trackPaths(),
            parameters.concurrency(),
            executorService
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            bellmanFord::compute,
            jobId
        );
    }

    CompletableFuture<HugeLongArray> breadthFirstSearch(
        GraphName graphName,
        GraphParameters graphParameters,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            BFSProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            ),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create the algorithm
        var exitAndAggregationConditions = ExitAndAggregation.create(graph, parameters);
        var mappedStartNodeId = graph.toMappedNodeId(parameters.sourceNode());

        var bfs = BFS.create(
            graph,
            mappedStartNodeId,
            exitAndAggregationConditions.exitFunction(),
            exitAndAggregationConditions.aggregatorFunction(),
            parameters.maxDepth(),
            executorService,
            parameters.concurrency(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            bfs::compute,
            jobId
        );
    }

    CompletableFuture<PathFindingResult> deltaStepping(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        DeltaSteppingParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            DeltaSteppingProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new SourceNodeGraphStoreValidation(parameters.sourceNode()),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create the algorithm
        var deltaStepping = DeltaStepping.of(graph, parameters, executorService, progressTracker);

        // Submit the algorithm for async computation

        return algorithmCaller.run(
            deltaStepping::compute,
            jobId
        );
    }

    CompletableFuture<HugeLongArray> depthFirstSearch(
        GraphName graphName,
        GraphParameters graphParameters,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress

    ) {
        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            DFSProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new SourceNodeTargetNodesGraphStoreValidation(
                parameters.sourceNode(),
                parameters.targetNodes()
            ),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create the algorithm
        var exitAndAggregationConditions = ExitAndAggregation.create(graph, parameters);
        var mappedStartNodeId = graph.toMappedNodeId(parameters.sourceNode());

        var dfs = new DFS(
            graph,
            mappedStartNodeId,
            exitAndAggregationConditions.exitFunction(),
            exitAndAggregationConditions.aggregatorFunction(),
            parameters.maxDepth(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            dfs::compute,
            jobId
        );
    }

    CompletableFuture<SpanningTree> kSpanningTree(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        KSpanningTreeParameters parameters,
        JobId jobId, boolean logProgress
    ) {
        // Fetch the Graph the algorithm will operate on
        var graph = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new KSpanningTreeGraphStoreValidation(parameters.sourceNode()),
            Optional.empty(),
            Optional.empty(),
            user,
            databaseId
        ).graph();

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            KSpanningTreeTask.create(graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var kSpanningTree = new KSpanningTree(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.k(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            kSpanningTree::compute,
            jobId
        );
    }

    CompletableFuture<PathFindingResult> longestPath() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<Stream<long[]>> randomWalk() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<HugeAtomicLongArray> randomWalkCountingNodeVisits() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<PrizeSteinerTreeResult> pcst() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathAStar() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathDijkstra() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathYens() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<PathFindingResult> singleSourceShortestPathDijkstra() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<SpanningTree> spanningTree() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<SteinerTreeResult> steinerTree() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

    CompletableFuture<TopologicalSortResult> topologicalSort() {
        // Create ProgressTracker
        // Fetch the Graph the algorithm will operate on
        // Create the algorithm
        // Submit the algorithm for async computation

        return CompletableFuture.failedFuture(new RuntimeException("Not yet implemented"));
    }

}
