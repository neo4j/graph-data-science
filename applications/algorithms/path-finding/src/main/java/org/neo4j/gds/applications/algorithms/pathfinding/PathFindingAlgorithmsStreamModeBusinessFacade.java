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
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
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

    public PathFindingAlgorithmsStreamModeBusinessFacade(
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
    }

    public <RESULT> RESULT allShortestPaths(
        GraphName graphName,
        AllShortestPathsConfig configuration,
        ResultBuilder<AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            AllShortestPaths,
            estimationFacade::allShortestPaths,
            graph -> pathFindingAlgorithms.allShortestPaths(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordStreamConfig configuration,
        ResultBuilder<AllShortestPathsBellmanFordStreamConfig, BellmanFordResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            graph -> pathFindingAlgorithms.bellmanFord(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT breadthFirstSearch(
        GraphName graphName,
        BfsStreamConfig configuration,
        ResultBuilder<BfsStreamConfig, HugeLongArray, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            BFS,
            estimationFacade::breadthFirstSearch,
            graph -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaStreamConfig configuration,
        ResultBuilder<AllShortestPathsDeltaStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            DeltaStepping,
            estimationFacade::deltaStepping,
            graph -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT depthFirstSearch(
        GraphName graphName,
        DfsStreamConfig configuration,
        ResultBuilder<DfsStreamConfig, HugeLongArray, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            DFS,
            estimationFacade::depthFirstSearch,
            graph -> pathFindingAlgorithms.depthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT longestPath(
        GraphName graphName,
        DagLongestPathStreamConfig configuration,
        ResultBuilder<DagLongestPathStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            LongestPath,
            estimationFacade::longestPath,
            graph -> pathFindingAlgorithms.longestPath(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT randomWalk(
        GraphName graphName,
        RandomWalkStreamConfig configuration,
        ResultBuilder<RandomWalkStreamConfig, Stream<long[]>, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            RandomWalk,
            () -> estimationFacade.randomWalk(configuration),
            graph -> pathFindingAlgorithms.randomWalk(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarStreamConfig configuration,
        ResultBuilder<ShortestPathAStarStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            AStar,
            estimationFacade::singlePairShortestPathAStar,
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraStreamConfig configuration,
        ResultBuilder<ShortestPathDijkstraStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            Dijkstra,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensStreamConfig configuration,
        ResultBuilder<ShortestPathYensStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            Yens,
            () -> estimationFacade.singlePairShortestPathYens(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraStreamConfig configuration,
        ResultBuilder<AllShortestPathsDijkstraStreamConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeStreamConfig configuration,
        ResultBuilder<SpanningTreeStreamConfig, SpanningTree, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            LabelForProgressTracking.SpanningTree,
            estimationFacade::spanningTree,
            graph -> pathFindingAlgorithms.spanningTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeStreamConfig configuration,
        ResultBuilder<SteinerTreeStreamConfig, SteinerTreeResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            SteinerTree,
            () -> estimationFacade.steinerTree(configuration),
            graph -> pathFindingAlgorithms.steinerTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT topologicalSort(
        GraphName graphName,
        TopologicalSortStreamConfig configuration,
        ResultBuilder<TopologicalSortStreamConfig, TopologicalSortResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsOrStreamMode(
            graphName,
            configuration,
            TopologicalSort,
            estimationFacade::topologicalSort,
            graph -> pathFindingAlgorithms.topologicalSort(graph, configuration),
            resultBuilder
        );
    }
}
