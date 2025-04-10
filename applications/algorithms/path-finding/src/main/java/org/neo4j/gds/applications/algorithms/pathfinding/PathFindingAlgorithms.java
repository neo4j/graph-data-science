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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.AStarParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraSourceTargetParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BFS;
import org.neo4j.gds.paths.traverse.DFS;
import org.neo4j.gds.paths.traverse.ExitAndAggregation;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensParameters;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.pricesteiner.PCSTFast;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeParameters;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisits;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * Here is the bottom business facade for path finding (or top layer in another module, or maybe not even a facade, ...).
 * As such, it is purely about calling algorithms and functional algorithm things.
 * The layers above will do input validation and result shaping.
 * For example, at this point we have stripped off modes. Modes are a result rendering or marshalling concept,
 * where you _use_ the results computed here, and ETL them.
 * Associated mode-specific validation is also done in layers above.
 */
public class PathFindingAlgorithms {

    Stream<AllShortestPathsStreamResult> allShortestPaths(
        Graph graph,
        AllShortestPathsParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = MSBFSASPAlgorithmFactory.create(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );
        return algorithm.compute();
    }

    public BellmanFordResult bellmanFord(
        Graph graph,
        BellmanFordParameters parameters,
        ProgressTracker progressTracker
    ) {
        var algorithm = new BellmanFord(
            graph,
            progressTracker,
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.trackNegativeCycles(),
            parameters.trackPaths(),
            parameters.concurrency()
        );

        return algorithm.compute();
    }

    /**
     * Here is an example of how resource management and structure collide.
     * Progress tracker is constructed here for BreadthFirstSearch, then inside it is delegated to BFS.
     * Ergo we apply the progress tracker resource machinery inside.
     * But it is not great innit.
     */
    HugeLongArray breadthFirstSearch(
        Graph graph,
        TraversalParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var exitAndAggregationConditions = ExitAndAggregation.create(graph, parameters);
        var mappedStartNodeId = graph.toMappedNodeId(parameters.sourceNode());

        var bfs = BFS.create(
            graph,
            mappedStartNodeId,
            exitAndAggregationConditions.exitFunction(),
            exitAndAggregationConditions.aggregatorFunction(),
            parameters.concurrency(),
            progressTracker,
            parameters.maxDepth(),
            terminationFlag
        );

        return bfs.compute();
    }

    public PathFindingResult deltaStepping(
        Graph graph,
        DeltaSteppingParameters parameters,
        ProgressTracker progressTracker,
        ExecutorService instance
    ) {
        var algorithm = DeltaStepping.of(graph, parameters, instance, progressTracker);

        return algorithm.compute();
    }

    HugeLongArray depthFirstSearch(
        Graph graph,
        TraversalParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var exitAndAggregationParameters = ExitAndAggregation.create(graph, parameters);
        var mappedSourceNodeId = graph.toMappedNodeId(parameters.sourceNode());

        var dfs = new DFS(
            graph,
            mappedSourceNodeId,
            exitAndAggregationParameters.exitFunction(),
            exitAndAggregationParameters.aggregatorFunction(),
            parameters.maxDepth(),
            progressTracker,
            terminationFlag
        );

        return dfs.compute();
    }

    public SpanningTree kSpanningTree(
        Graph graph,
        KSpanningTreeParameters parameters,
        ProgressTracker progressTracker, TerminationFlag terminationFlag
    ) {
        var algorithm = new KSpanningTree(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.k(),
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public PathFindingResult longestPath(
        Graph graph,
        DagLongestPathParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = new DagLongestPath(
            graph,
            progressTracker,
            parameters.concurrency(),
            terminationFlag
        );

        return algorithm.compute();
    }

    public Stream<long[]> randomWalk(
        Graph graph,
        RandomWalkParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = RandomWalk.create(
            graph,
            parameters.concurrency(),
            parameters.walkParameters(),
            parameters.sourceNodes(),
            parameters.walkBufferSize(),
            parameters.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithm.compute();
    }

    PrizeSteinerTreeResult pcst(Graph graph, PCSTParameters parameters, ProgressTracker progressTracker) {
        var prizeProperty = graph.nodeProperties(parameters.prizeProperty());
        var algorithm = new PCSTFast(
            graph,
            (v) -> Math.max(prizeProperty.doubleValue(v), 0),
            progressTracker
        );

        return algorithm.compute();
    }

    HugeAtomicLongArray randomWalkCountingNodeVisits(
        Graph graph,
        RandomWalkParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        ExecutorService executorService
    ) {
        var algorithm = RandomWalkCountingNodeVisits.create(
            graph,
            parameters.concurrency(),
            parameters.walkParameters(),
            parameters.sourceNodes(),
            parameters.randomSeed(),
            progressTracker,
            executorService,
            terminationFlag
        );

        return algorithm.compute();
    }

    public PathFindingResult singlePairShortestPathAStar(
        Graph graph,
        AStarParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {

        var algorithm = AStar.sourceTarget(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    PathFindingResult singlePairShortestPathDijkstra(
        Graph graph,
        DijkstraSourceTargetParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {

        var algorithm = Dijkstra.sourceTarget(
            graph,
            parameters.sourceNode(),
            parameters.targetsList(),
            false,
            Optional.empty(),
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public PathFindingResult singlePairShortestPathYens(
        Graph graph,
        YensParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = Yens.sourceTarget(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    PathFindingResult singleSourceShortestPathDijkstra(
        Graph graph,
        long originalNodeId,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = Dijkstra.singleSource(
            graph,
            originalNodeId,
            false,
            Optional.empty(),
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public SpanningTree spanningTree(
        Graph graph,
        SpanningTreeParameters parameters,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var algorithm = new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public SteinerTreeResult steinerTree(
        Graph graph, SteinerTreeParameters parameters,
        ProgressTracker progressTracker, TerminationFlag terminationFlag
    ) {
        var mappedSourceNodeId = graph.toMappedNodeId(parameters.sourceNode());
        var mappedTargetNodeIds = parameters.targetNodes()
            .stream()
            .map(graph::safeToMappedNodeId)
            .toList();

        var algorithm = new ShortestPathsSteinerAlgorithm(
            graph,
            mappedSourceNodeId,
            mappedTargetNodeIds,
            parameters.delta(),
            parameters.concurrency(),
            parameters.applyRerouting(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithm.compute();
    }

    public TopologicalSortResult topologicalSort(
        Graph graph,
        TopologicalSortParameters parameters,
        ProgressTracker progressTracker, TerminationFlag terminationFlag
    ) {
        var algorithm = new TopologicalSort(
            graph,
            progressTracker,
            parameters.concurrency(),
            parameters.computeMaxDistanceFromSource(),
            terminationFlag
        );

        return algorithm.compute();
    }

}
