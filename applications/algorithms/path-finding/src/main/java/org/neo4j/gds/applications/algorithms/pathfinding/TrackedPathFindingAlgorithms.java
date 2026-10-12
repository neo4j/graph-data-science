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

import org.neo4j.gds.PathFindingAlgorithmTasks;
import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.dag.longestPath.DagLongestPathBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PrizeCollectingSteinerTreeResult;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.stream.Stream;

class TrackedPathFindingAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final PathFindingAlgorithms algorithms;
    private final RequestScopedDependencies requestScopedDependencies;
    private final ProgressTrackerCreator progressTrackerCreator;

    TrackedPathFindingAlgorithms(
        PathFindingAlgorithms algorithms,
        RequestScopedDependencies requestScopedDependencies,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.algorithms = algorithms;
        this.requestScopedDependencies = requestScopedDependencies;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    Stream<AllShortestPathsStreamResult> allShortestPaths(Graph graph, AllShortestPathsConfig configuration) {
        var progressTracker = ProgressTracker.NULL_TRACKER;

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.allShortestPaths(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            true
        );
    }

    BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.bellmanFord(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.bellmanFord(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            false
        );
    }

    HugeLongArray breadthFirstSearch(Graph graph, BfsBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.bfs(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.breadthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.deltaStepping(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.deltaStepping(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.dfs(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.depthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.kSpanningTree(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.kSpanningTree(
                graph,
                configuration.toKSpanningTreeParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    PathFindingResult longestPath(Graph graph, DagLongestPathBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.longestPath(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.longestPath(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    FlowResult maxFlow(Graph graph, MaxFlowBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.maxFlow(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.maxFlow(
                graph,
                configuration.toMaxFlowParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    CostFlowResult minCostMaxFlow(Graph graphOfFlows, Graph graphOfCosts, MCMFBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.minCostMaxFlow(configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.minCostMaxFlow(
                graphOfFlows,
                graphOfCosts,
                configuration.toMCMFParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    PrizeCollectingSteinerTreeResult prizeCollectingSteinerTree(Graph graph, PCSTBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.prizeCollectingSteinerTree(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.prizeCollectingSteinerTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.randomWalk(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.randomWalk(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            false
        );
    }

    HugeAtomicLongArray randomWalkCountingNodeVisits(Graph graph, RandomWalkBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.randomWalkCountingVisits(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.randomWalkCountingNodeVisits(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            false // I mean it is a tossup; unused, duplicated code gives no indication, but original had false here, so...
        );
    }

    PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.aStar(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathAStar(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            false
        );
    }

    PathFindingResult singlePairShortestPathDijkstra(Graph graph, DijkstraSourceTargetsBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.dijkstra(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathDijkstra(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            false
        );
    }

    PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.yens(graph, configuration.concurrency(), configuration.k());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathYens(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.singleSourceDijkstra(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.singleSourceShortestPathDijkstra(
                graph,
                configuration.sourceNode(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            false
        );
    }

    public SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.spanningTree(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.spanningTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }

    public SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var task = PathFindingAlgorithmTasks.steinerTree(parameters, graph);
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.steinerTree(
                graph,
                parameters,
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            true
        );
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.topologicalSort(graph, configuration.concurrency());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.topologicalSort(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            true
        );
    }
}
