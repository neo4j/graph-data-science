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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
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
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

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

    public BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = PathFindingAlgorithmTasks.bellmanFord(configuration.concurrency());
        var progressTracker = createProgressTracker(task, configuration);


        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.bellmanFord(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, false);
    }

    public PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var progressTracker = createProgressTracker(PathFindingAlgorithmTasks.deltaStepping(configuration.concurrency()), configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.deltaStepping(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    public SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.kSpanningTree(graph, configuration.concurrency()),
            configuration
        );

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.kSpanningTree(
                graph,
                configuration.toKSpanningTreeParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker, true);
    }

    CostFlowResult mcmf(GraphStore graphStore, MCMFBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.minCostMaxFlow(configuration.concurrency()),
            configuration
        );

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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
        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
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
