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
import org.neo4j.gds.core.loading.NoAlgorithmValidation;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.termination.TerminationFlag;

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

    CompletableFuture<HugeLongArray> breadthFirstSearch() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> deltaStepping() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<HugeLongArray> depthFirstSearch() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SpanningTree> kSpanningTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> longestPath() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<Stream<long[]>> randomWalk() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<HugeAtomicLongArray> randomWalkCountingNodeVisits() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PrizeSteinerTreeResult> pcst() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathAStar() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathDijkstra() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singlePairShortestPathYens() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<PathFindingResult> singleSourceShortestPathDijkstra() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SpanningTree> spanningTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<SteinerTreeResult> steinerTree() {
        throw new RuntimeException("Not yet implemented");
    }

    CompletableFuture<TopologicalSortResult> topologicalSort() {
        throw new RuntimeException("Not yet implemented");
    }

}
