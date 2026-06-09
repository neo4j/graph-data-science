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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
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
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.stream.Stream;

public class PathFindingAlgorithmsBusinessFacade {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final PathFindingAlgorithms algorithms;

    // request scoped parameters
    private final RequestScopedDependencies requestScopedDependencies;
    private final ProgressTrackerCreator progressTrackerCreator;

    public PathFindingAlgorithmsBusinessFacade(
        PathFindingAlgorithms algorithms, RequestScopedDependencies requestScopedDependencies,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.algorithms = algorithms;
        this.requestScopedDependencies = requestScopedDependencies;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    Stream<AllShortestPathsStreamResult> allShortestPaths(Graph graph, AllShortestPathsConfig configuration) {
        var progressTracker = ProgressTracker.NULL_TRACKER;

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.allShortestPaths(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ), progressTracker, true);
    }

    public BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.bellmanFord(configuration.concurrency());
        var progressTracker = createProgressTracker(task, configuration);


        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.bellmanFord(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, false);
    }

    /**
     * Here is an example of how resource management and structure collide.
     * Progress tracker is constructed here for BreadthFirstSearch, then inside it is delegated to BFS.
     * Ergo we apply the progress tracker resource machinery inside.
     * But it is not great innit.
     */
    HugeLongArray breadthFirstSearch(Graph graph, BfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(PathFindingAlgorithmTasks.bfs(configuration.concurrency()), configuration);

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.breadthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    public PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var progressTracker = createProgressTracker(PathFindingAlgorithmTasks.deltaStepping(configuration.concurrency()), configuration);

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.deltaStepping(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    /**
     * Moar resource shenanigans
     *
     * @see #breadthFirstSearch(org.neo4j.gds.api.Graph, org.neo4j.gds.paths.traverse.BfsBaseConfig)
     */
    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(PathFindingAlgorithmTasks.dfs(configuration.concurrency()), configuration);

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.depthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    public SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.kSpanningTree(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.kSpanningTree(
                graph,
                configuration.toKSpanningTreeParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    PathFindingResult longestPath(Graph graph, DagLongestPathBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.longestPath(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.longestPath(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    FlowResult maxFlow(Graph graph, MaxFlowBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.maxFlow(configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.maxFlow(
                graph,
                configuration.toMaxFlowParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    CostFlowResult mcmf(GraphStore graphStore, MCMFBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.minCostMaxFlow(configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.mcmf(
                graphStore,
                configuration.relationshipWeightProperty(),
                configuration.costProperty(),
                configuration.nodeLabelIdentifiers(graphStore),
                configuration.internalRelationshipTypes(graphStore),
                configuration.toMCMFParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.randomWalk(graph, configuration.concurrency());
        var progressTracker = createProgressTracker(task, configuration);

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.randomWalk(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ), progressTracker, false);
    }

    HugeAtomicLongArray randomWalkCountingNodeVisits(Graph graph, RandomWalkBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.randomWalkCountingVisits(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.randomWalkCountingNodeVisits(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ), progressTracker, true);
    }

    PrizeSteinerTreeResult pcst(Graph graph, PCSTBaseConfig configuration) {

        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.pcst(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.pcst(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    public PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.aStar(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathAStar(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, false);
    }

    PathFindingResult singlePairShortestPathDijkstra(Graph graph, DijkstraSourceTargetsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.dijkstra(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathDijkstra(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, false);

    }

    public PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {

        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.yens(graph, configuration.concurrency(), configuration.k()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.singlePairShortestPathYens(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.singleSourceDijkstra(graph, configuration.concurrency()),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.singleSourceShortestPathDijkstra(
                graph,
                configuration.sourceNode(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, false);
    }

    public SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.spanningTree(graph, configuration.concurrency()),
            configuration
        );
        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.spanningTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    public SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.steinerTree(parameters, graph),
            configuration
        );

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.steinerTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ), progressTracker, true);
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.topologicalSort(graph, configuration.concurrency());
        var progressTracker = createProgressTracker(task, configuration);

        return algorithmMachinery.getResultAndManageProgressTracker(
            () -> algorithms.topologicalSort(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    private ProgressTracker createProgressTracker(Task task, AlgoBaseConfig configuration) {
        return progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );
    }
}
