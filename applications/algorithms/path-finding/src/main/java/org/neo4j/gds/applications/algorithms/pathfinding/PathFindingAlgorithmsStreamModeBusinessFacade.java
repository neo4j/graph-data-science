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
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
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

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.AStar;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.AllShortestPaths;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BFS;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BellmanFord;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DFS;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.LongestPath;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.RandomWalk;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SteinerTree;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.TopologicalSort;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Yens;

/**
 * Here is the top level business facade for all your path finding stream needs.
 * It will have all pathfinding algorithms on it, in stream mode.
 */
public class PathFindingAlgorithmsStreamModeBusinessFacade {
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    PathFindingAlgorithmsStreamModeBusinessFacade(
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
    }

    public <RESULT> Stream<RESULT> allShortestPaths(
        GraphName graphName,
        AllShortestPathsConfig configuration,
        StreamResultBuilder<AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            AllShortestPaths,
            estimationFacade::allShortestPaths,
            (graph, __) -> pathFindingAlgorithms.allShortestPaths(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsBellmanFordStreamConfig, BellmanFordResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            (graph, __) -> pathFindingAlgorithms.bellmanFord(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> breadthFirstSearch(
        GraphName graphName,
        BfsStreamConfig configuration,
        StreamResultBuilder<BfsStreamConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            BFS,
            estimationFacade::breadthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsDeltaStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DeltaStepping,
            estimationFacade::deltaStepping,
            (graph, __) -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> depthFirstSearch(
        GraphName graphName,
        DfsStreamConfig configuration,
        StreamResultBuilder<DfsStreamConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            DFS,
            estimationFacade::depthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.depthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> longestPath(
        GraphName graphName,
        DagLongestPathStreamConfig configuration,
        StreamResultBuilder<DagLongestPathStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LongestPath,
            estimationFacade::longestPath,
            (graph, __) -> pathFindingAlgorithms.longestPath(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> randomWalk(
        GraphName graphName,
        RandomWalkStreamConfig configuration,
        StreamResultBuilder<RandomWalkStreamConfig, Stream<long[]>, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            RandomWalk,
            () -> estimationFacade.randomWalk(configuration),
            (graph, __) -> pathFindingAlgorithms.randomWalk(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarStreamConfig configuration,
        StreamResultBuilder<ShortestPathAStarStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            AStar,
            estimationFacade::singlePairShortestPathAStar,
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraStreamConfig configuration,
        StreamResultBuilder<ShortestPathDijkstraStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Dijkstra,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensStreamConfig configuration,
        StreamResultBuilder<ShortestPathYensStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Yens,
            () -> estimationFacade.singlePairShortestPathYens(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder

        );
    }

    public <RESULT> Stream<RESULT> singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraStreamConfig configuration,
        StreamResultBuilder<AllShortestPathsDijkstraStreamConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> spanningTree(
        GraphName graphName,
        SpanningTreeStreamConfig configuration,
        StreamResultBuilder<SpanningTreeStreamConfig, SpanningTree, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LabelForProgressTracking.SpanningTree,
            estimationFacade::spanningTree,
            (graph, __) -> pathFindingAlgorithms.spanningTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> steinerTree(
        GraphName graphName,
        SteinerTreeStreamConfig configuration,
        StreamResultBuilder<SteinerTreeStreamConfig, SteinerTreeResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SteinerTree,
            () -> estimationFacade.steinerTree(configuration),
            (graph, __) -> pathFindingAlgorithms.steinerTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> topologicalSort(
        GraphName graphName,
        TopologicalSortStreamConfig configuration,
        StreamResultBuilder<TopologicalSortStreamConfig, TopologicalSortResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            TopologicalSort,
            estimationFacade::topologicalSort,
            (graph, __) -> pathFindingAlgorithms.topologicalSort(graph, configuration),
            resultBuilder
        );
    }
}
