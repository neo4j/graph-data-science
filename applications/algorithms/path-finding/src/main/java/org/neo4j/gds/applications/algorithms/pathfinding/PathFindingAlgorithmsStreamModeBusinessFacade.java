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

import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStreamConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.traverse.DfsStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeStreamConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.AStar;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.AllShortestPaths;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.BFS;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.BellmanFord;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.DFS;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.LongestPath;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.RandomWalk;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SteinerTree;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.TopologicalSort;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Yens;

/**
 * Here is the top level business facade for all your path finding stream needs.
 * It will have all pathfinding algorithms on it, in stream mode.
 */
public class PathFindingAlgorithmsStreamModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimation;
    private final PathFindingAlgorithms algorithms;

    private final AlgorithmProcessingTemplateConvenience convenience;

    PathFindingAlgorithmsStreamModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimation,
        PathFindingAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience convenience
    ) {
        this.convenience = convenience;
        this.algorithms = algorithms;
        this.estimation = estimation;
    }

    public <RESULT> Stream<RESULT> allShortestPaths(
        GraphName graphName,
        AllShortestPathsConfig configuration,
        StreamResultBuilder<AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            AllShortestPaths,
            estimation::allShortestPaths,
            (graph, __) -> algorithms.allShortestPaths(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsBellmanFordStreamConfig, BellmanFordResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimation.bellmanFord(configuration),
            (graph, __) -> algorithms.bellmanFord(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> breadthFirstSearch(
        GraphName graphName,
        BfsStreamConfig configuration,
        StreamResultBuilder<BfsStreamConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BFS,
            estimation::breadthFirstSearch,
            (graph, __) -> algorithms.breadthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsDeltaStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DeltaStepping,
            estimation::deltaStepping,
            (graph, __) -> algorithms.deltaStepping(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> depthFirstSearch(
        GraphName graphName,
        DfsStreamConfig configuration,
        StreamResultBuilder<DfsStreamConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DFS,
            estimation::depthFirstSearch,
            (graph, __) -> algorithms.depthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> longestPath(
        GraphName graphName,
        DagLongestPathStreamConfig configuration,
        StreamResultBuilder<DagLongestPathStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LongestPath,
            estimation::longestPath,
            (graph, __) -> algorithms.longestPath(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> randomWalk(
        GraphName graphName,
        RandomWalkStreamConfig configuration,
        StreamResultBuilder<RandomWalkStreamConfig, Stream<long[]>, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            RandomWalk,
            () -> estimation.randomWalk(configuration),
            (graph, __) -> algorithms.randomWalk(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarStreamConfig configuration,
        StreamResultBuilder<ShortestPathAStarStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            AStar,
            estimation::singlePairShortestPathAStar,
            (graph, __) -> algorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraStreamConfig configuration,
        StreamResultBuilder<ShortestPathDijkstraStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Dijkstra,
            () -> estimation.singlePairShortestPathDijkstra(configuration),
            (graph, __) -> algorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensStreamConfig configuration,
        StreamResultBuilder<ShortestPathYensStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Yens,
            () -> estimation.singlePairShortestPathYens(configuration),
            (graph, __) -> algorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder

        );
    }

    public <RESULT> Stream<RESULT> singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsDijkstraStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimation.singleSourceShortestPathDijkstra(configuration),
            (graph, __) -> algorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> spanningTree(
        GraphName graphName,
        SpanningTreeStreamConfig configuration,
        StreamResultBuilder<SpanningTreeStreamConfig, SpanningTree, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Algorithm.SpanningTree,
            estimation::spanningTree,
            (graph, __) -> algorithms.spanningTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> steinerTree(
        GraphName graphName,
        SteinerTreeStreamConfig configuration,
        StreamResultBuilder<SteinerTreeStreamConfig, SteinerTreeResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SteinerTree,
            () -> estimation.steinerTree(configuration),
            (graph, __) -> algorithms.steinerTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> topologicalSort(
        GraphName graphName,
        TopologicalSortStreamConfig configuration,
        StreamResultBuilder<TopologicalSortStreamConfig, TopologicalSortResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            TopologicalSort,
            estimation::topologicalSort,
            (graph, __) -> algorithms.topologicalSort(graph, configuration),
            resultBuilder
        );
    }
}
