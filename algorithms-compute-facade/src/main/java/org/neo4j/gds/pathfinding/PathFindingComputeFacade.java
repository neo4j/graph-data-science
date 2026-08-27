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

import org.neo4j.gds.PathFindingAlgorithmTasks;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.pathfinding.MSBFSASPAlgorithmFactory;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlow;
import org.neo4j.gds.maxflow.MaxFlowParameters;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFParameters;
import org.neo4j.gds.mcmf.MinCostMaxFlow;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.ExitAndAggregation;
import org.neo4j.gds.paths.traverse.bfs.BFS;
import org.neo4j.gds.paths.traverse.dfs.DFS;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class PathFindingComputeFacade {
    private final Log log;

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;
    // This service is what the algorithms use for parallelism.
    private final ExecutorService executorService;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final TerminationFlag terminationFlag;

    public PathFindingComputeFacade(
        Log log,
        AsyncAlgorithmCaller algorithmCaller,
        ExecutorService executorService,
        TerminationFlag terminationFlag,
        ProgressTrackerFactory progressTrackerFactory
    ) {
        this.log = log;
        this.algorithmCaller = algorithmCaller;
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.progressTrackerFactory = progressTrackerFactory;
    }

    public CompletableFuture<TimedAlgorithmResult<Stream<AllShortestPathsStreamResult>>> allShortestPaths(
        Graph graph,
        AllShortestPathsParameters parameters,
        JobId jobId
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Stream.empty()));
        }

        // Create ProgressTracker
        // `allShortestPaths` doesn't use progress tracker (yet 🤔)
        var progressTracker = ProgressTracker.NULL_TRACKER;

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

    public CompletableFuture<TimedAlgorithmResult<HugeLongArray>> breadthFirstSearch(
        Graph graph,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(HugeLongArray.newArray(0L)));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.bfs(parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

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

    public CompletableFuture<TimedAlgorithmResult<HugeLongArray>> depthFirstSearch(
        Graph graph,
        TraversalParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(HugeLongArray.newArray(0L)));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.dfs(parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

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

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> longestPath(
        Graph graph,
        DagLongestPathParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.longestPath(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var dagLongestPath = new DagLongestPath(
            graph,
            progressTracker,
            parameters.concurrency(),
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            dagLongestPath::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<FlowResult>> maxFlow(
        Graph graph,
        MaxFlowParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(FlowResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.maxFlow(parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var algo = MaxFlow.create(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            algo::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<CostFlowResult>> mcmf(
        Graph capacityGraph,
        Graph costGraph,
        MCMFParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (capacityGraph.isEmpty() || costGraph.isEmpty()) { //WIP
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(CostFlowResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.minCostMaxFlow(parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var algo = MinCostMaxFlow.create(
            capacityGraph,
            costGraph,
            parameters,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            algo::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<Stream<long[]>>> randomWalk(
        Graph graph,
        RandomWalkParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Stream.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.randomWalk(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );
        // Create the algorithm
        var randomWalk = RandomWalk.create(
            log,
            graph,
            parameters,
            progressTracker,
            executorService,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            randomWalk::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<TopologicalSortResult>> topologicalSort(
        Graph graph,
        TopologicalSortParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(TopologicalSortResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.topologicalSort(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var topologicalSort = new TopologicalSort(
            graph,
            progressTracker,
            parameters.concurrency(),
            parameters.computeMaxDistanceFromSource(),
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            topologicalSort::compute,
            jobId
        );
    }
}
