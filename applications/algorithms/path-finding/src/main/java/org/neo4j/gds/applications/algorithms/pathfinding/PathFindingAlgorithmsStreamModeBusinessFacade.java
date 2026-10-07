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
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.dag.longestPath.DagLongestPathBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStreamConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.pcst.PCSTStreamConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BellmanFord;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PCST;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SingleSourceDijkstra;

/**
 * Here is the top level business facade for all your path finding stream needs.
 * It will have all pathfinding algorithms on it, in stream mode.
 */
public class PathFindingAlgorithmsStreamModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimation;
    private final PathFindingAlgorithmsBusinessFacade algorithms;

    private final AlgorithmProcessingTemplateConvenience convenience;
    private final InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms;
    private final Synchroniser synchroniser;

    PathFindingAlgorithmsStreamModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimation,
        PathFindingAlgorithmsBusinessFacade algorithms,
        AlgorithmProcessingTemplateConvenience convenience,
        InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms,
        Synchroniser synchroniser
    ) {
        this.convenience = convenience;
        this.algorithms = algorithms;
        this.estimation = estimation;
        this.instrumentedPathFindingAlgorithms = instrumentedPathFindingAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> Stream<RESULT> allShortestPaths(
        GraphName graphName,
        AllShortestPathsConfig configuration,
        StreamResultBuilder<Stream<AllShortestPathsStreamResult>, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.allShortestPaths(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordStreamConfig configuration,
        StreamResultBuilder<BellmanFordResult, RESULT> resultBuilder
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
        BfsBaseConfig configuration,
        StreamResultBuilder<HugeLongArray, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.bfs(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaStreamConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
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
        DfsBaseConfig configuration,
        StreamResultBuilder<HugeLongArray, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.dfs(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> longestPath(
        GraphName graphName,
        DagLongestPathBaseConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.longestPath(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> maxFlow(
        GraphName graphName,
        MaxFlowBaseConfig configuration,
        StreamResultBuilder<FlowResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.maxFlow(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> minCostMaxFlow(
        GraphName graphName,
        MCMFBaseConfig configuration,
        StreamResultBuilder<CostFlowResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.minCostMaxFlow(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> pcst(
        GraphName graphName,
        PCSTStreamConfig configuration,
        StreamResultBuilder<PrizeSteinerTreeResult, RESULT> resultBuilder
    ) {
        return convenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            PCST,
            estimation::pcst,
            (graph, __) -> algorithms.pcst(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> Stream<RESULT> randomWalk(
        GraphName graphName,
        RandomWalkBaseConfig configuration,
        StreamResultBuilder<Stream<long[]>, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.randomWalk(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarBaseConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathAStar(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathDijkstra(
        GraphName graphName,
        DijkstraSourceTargetsBaseConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathDijkstra(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensBaseConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathYens(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraStreamConfig configuration,
        StreamResultBuilder<PathFindingResult, RESULT> resultBuilder
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
        SpanningTreeBaseConfig configuration,
        StreamResultBuilder<SpanningTree, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.spanningTree(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> steinerTree(
        GraphName graphName,
        SteinerTreeBaseConfig configuration,
        StreamResultBuilder<SteinerTreeResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.steinerTree(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> Stream<RESULT> topologicalSort(
        GraphName graphName,
        TopologicalSortBaseConfig configuration,
        StreamResultBuilder<TopologicalSortResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.topologicalSort(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }
}
