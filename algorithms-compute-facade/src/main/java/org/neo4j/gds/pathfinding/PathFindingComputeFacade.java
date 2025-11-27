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

import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.pathfinding.MSBFSASPAlgorithmFactory;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.longestPath.LongestPathTask;
import org.neo4j.gds.dag.topologicalsort.TopSortTask;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.kspanningtree.KSpanningTreeTask;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlow;
import org.neo4j.gds.maxflow.MaxFlowParameters;
import org.neo4j.gds.paths.RelationshipCountProgressTaskFactory;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.AStarParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSourceTargetParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BFS;
import org.neo4j.gds.paths.traverse.BFSProgressTask;
import org.neo4j.gds.paths.traverse.DFS;
import org.neo4j.gds.paths.traverse.DFSProgressTask;
import org.neo4j.gds.paths.traverse.ExitAndAggregation;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensParameters;
import org.neo4j.gds.paths.yens.YensProgressTask;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.pricesteiner.PCSTFast;
import org.neo4j.gds.pricesteiner.PCSTProgressTrackerTaskCreator;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeParameters;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeProgressTask;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisits;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisitsProgressTaskFactory;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.RandomWalkProgressTask;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class PathFindingComputeFacade {

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
        AsyncAlgorithmCaller algorithmCaller,
        ExecutorService executorService,
        TerminationFlag terminationFlag,
        ProgressTrackerFactory progressTrackerFactory
    ) {
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
        var progressTracker = progressTrackerFactory.nullTracker();

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

    public CompletableFuture<TimedAlgorithmResult<BellmanFordResult>> bellmanFord(
        Graph graph,
        BellmanFordParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(BellmanFordResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            BellmanFordProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

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
            BFSProgressTask.create(),
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

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> deltaStepping(
        Graph graph,
        DeltaSteppingParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            DeltaSteppingProgressTask.create(),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var deltaStepping = DeltaStepping.of(graph, parameters, executorService, progressTracker);

        // Submit the algorithm for async computation

        return algorithmCaller.run(
            deltaStepping::compute,
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
            DFSProgressTask.create(),
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

    public CompletableFuture<TimedAlgorithmResult<SpanningTree>> kSpanningTree(
        Graph graph,
        KSpanningTreeParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(SpanningTree.EMPTY));
        }

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
            LongestPathTask.create(graph.nodeCount()),
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
            null
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
        var progressTracker = ProgressTracker.NULL_TRACKER;

        // Create the algorithm
        var algo = new MaxFlow(
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
            RandomWalkProgressTask.create(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );
        // Create the algorithm
        var randomWalk = RandomWalk.create(
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

    public CompletableFuture<TimedAlgorithmResult<HugeAtomicLongArray>> randomWalkCountingNodeVisits(
        Graph graph,
        RandomWalkParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(
                HugeAtomicLongArray.of(
                    0,
                    ParalleLongPageCreator.passThrough(parameters.concurrency())
                )
            ));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            RandomWalkCountingNodeVisitsProgressTaskFactory.create(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var randomWalkCountingNodeVisits = RandomWalkCountingNodeVisits.create(
            graph,
            parameters,
            progressTracker,
            executorService,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            randomWalkCountingNodeVisits::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PrizeSteinerTreeResult>> pcst(
        Graph graph,
        PCSTParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PrizeSteinerTreeResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(), graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var prizeProperty = graph.nodeProperties(parameters.prizeProperty());
        var pcstFast = new PCSTFast(
            graph,
            (v) -> Math.max(prizeProperty.doubleValue(v), 0),
            progressTracker
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            pcstFast::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> singlePairShortestPathAStar(
        Graph graph,
        AStarParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.AStar, graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var aStar = AStar.sourceTarget(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            aStar::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> singlePairShortestPathDijkstra(
        Graph graph,
        DijkstraSourceTargetParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.Dijkstra, graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var dijkstra = DijkstraFactory.sourceTarget(
            graph,
            parameters.sourceNode(),
            parameters.targetsList(),
            false,
            Optional.empty(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            dijkstra::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> singlePairShortestPathYens(
        Graph graph,
        YensParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            YensProgressTask.create(
                graph.relationshipCount(),
                parameters.k()
            ),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var yens = Yens.sourceTarget(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            yens::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> singleSourceShortestPathDijkstra(
        Graph graph,
        DijkstraSingleSourceParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.SingleSourceDijkstra, graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var dijkstra = DijkstraFactory.singleSource(
            graph,
            parameters.sourceNode(),
            false,
            Optional.empty(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            dijkstra::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<SpanningTree>> spanningTree(
        Graph graph,
        SpanningTreeParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(SpanningTree.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.SpanningTree, graph.relationshipCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var prim = new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            prim::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<SteinerTreeResult>> steinerTree(
        Graph graph,
        SteinerTreeParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(SteinerTreeResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            SteinerTreeProgressTask.create(parameters, graph.nodeCount()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var mappedSourceNodeId = graph.toMappedNodeId(parameters.sourceNode());
        var mappedTargetNodeIds = parameters.targetNodes()
            .stream()
            .map(graph::safeToMappedNodeId)
            .toList();

        var steinerTree = new ShortestPathsSteinerAlgorithm(
            graph,
            mappedSourceNodeId,
            mappedTargetNodeIds,
            parameters.delta(),
            parameters.concurrency(),
            parameters.applyRerouting(),
            executorService,
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            steinerTree::compute,
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
            TopSortTask.create(graph),
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
