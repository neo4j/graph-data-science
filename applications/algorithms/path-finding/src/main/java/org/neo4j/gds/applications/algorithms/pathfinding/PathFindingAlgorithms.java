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

import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.allshortestpaths.MSBFSASPAlgorithm;
import org.neo4j.gds.allshortestpaths.MSBFSAllShortestPaths;
import org.neo4j.gds.allshortestpaths.WeightedAllShortestPaths;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.pathfinding.traverse.BreadthFirstSearch;
import org.neo4j.gds.applications.algorithms.pathfinding.traverse.DepthFirstSearch;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskTreeProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.A_STAR;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.BELLMAN_FORD;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.BFS;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DELTA_STEPPING;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DFS;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DIJKSTRA;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.K_SPANNING_TREE;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.LONGEST_PATH;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.RANDOM_WALK;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.SPANNING_TREE;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.STEINER;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.YENS;

/**
 * Here is the bottom business facade for path finding (or top layer in another module, or maybe not even a facade, ...).
 * As such, it is purely about calling algorithms and functional algorithm things.
 * The layers above will do input validation and result shaping.
 * For example, at this point we have stripped off modes. Modes are a result rendering or marshalling concept,
 * where you _use_ the results computed here, and ETL them.
 * Associated mode-specific validation is also done in layers above.
 */
public class PathFindingAlgorithms {
    // global scoped dependencies
    private final Log log;

    // request scoped parameters
    private final RequestScopedDependencies requestScopedDependencies;

    public PathFindingAlgorithms(
        Log log,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    Stream<AllShortestPathsStreamResult> allShortestPaths(Graph graph, AllShortestPathsConfig configuration) {
        var algorithm = selectAlgorithm(graph, configuration);

        return algorithm.compute();
    }

    BellmanFordResult bellmanFord(Graph graph, BellmanFordBaseConfig configuration) {
        var task = Tasks.iterativeOpen(
            BELLMAN_FORD,
            () -> List.of(
                Tasks.leaf("Relax"),
                Tasks.leaf("Sync")
            )
        );
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = new BellmanFord(
            graph,
            progressTracker,
            graph.toMappedNodeId(configuration.sourceNode()),
            configuration.trackNegativeCycles(),
            configuration.trackPaths(),
            configuration.concurrency()
        );

        return algorithm.compute();
    }

    HugeLongArray breadthFirstSearch(Graph graph, BfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(configuration, Tasks.leaf(BFS));

        var algorithm = new BreadthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker
        );
    }

    PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var iterativeTask = Tasks.iterativeOpen(
            DELTA_STEPPING,
            () -> List.of(
                Tasks.leaf(DeltaStepping.Phase.RELAX.name()),
                Tasks.leaf(DeltaStepping.Phase.SYNC.name())
            )
        );
        var progressTracker = createProgressTracker(configuration, iterativeTask);
        var algorithm = DeltaStepping.of(graph, configuration, DefaultPool.INSTANCE, progressTracker);
        return algorithm.compute();
    }

    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(configuration, Tasks.leaf(DFS));

        var algorithm = new DepthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker
        );
    }

    SpanningTree kSpanningTree(Graph graph, KSpanningTreeWriteConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The K-Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var parameters = configuration.toParameters();

        var progressTracker = createProgressTracker(configuration, Tasks.task(
            K_SPANNING_TREE,
            Tasks.leaf(SPANNING_TREE, graph.relationshipCount()),
            Tasks.leaf("Remove relationships")
        ));

        var algorithm = new KSpanningTree(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.k(),
            progressTracker
        );

        return algorithm.compute();
    }

    PathFindingResult longestPath(Graph graph, AlgoBaseConfig configuration) {
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var traversalTask = Tasks.leaf("Traversal", graph.nodeCount());
        var task = Tasks.task(LONGEST_PATH, List.of(initializationTask, traversalTask));
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = new DagLongestPath(
            graph,
            progressTracker,
            configuration.concurrency()
        );

        return algorithm.compute();
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            tasks.add(DegreeCentralityFactory.degreeCentralityProgressTask(graph));
        }
        tasks.add(Tasks.leaf("create walks", graph.nodeCount()));
        var progressTracker = createProgressTracker(configuration, Tasks.task(RANDOM_WALK, tasks));

        var algorithm = RandomWalk.create(
            graph,
            configuration.concurrency(),
            configuration.walkParameters(),
            configuration.sourceNodes(),
            configuration.walkBufferSize(),
            configuration.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE
        );

        return algorithm.compute();
    }

    PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(A_STAR, graph.relationshipCount())
        );

        var algorithm = AStar.sourceTarget(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithm.compute();
    }

    /**
     * This is conceptually very pretty, it speaks to you: call Dijkstra on a graph, return the result.
     * Caller is responsible for looking up the graph, including the (implicit) validation that that involves.
     * And they also get to do result rendering, using details of user request and GraphStore state.
     * Down here though it is just the algorithm.
     */
    PathFindingResult singlePairShortestPathDijkstra(Graph graph, DijkstraSourceTargetsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(DIJKSTRA, graph.relationshipCount())
        );

        var dijkstra = Dijkstra.sourceTarget(
            graph,
            configuration.sourceNode(),
            configuration.targetsList(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return dijkstra.compute();
    }

    PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {
        var initialTask = Tasks.leaf(DIJKSTRA, graph.relationshipCount());
        var pathGrowingTask = Tasks.leaf("Path growing", configuration.k() - 1);
        var yensTask = Tasks.task(YENS, initialTask, pathGrowingTask);

        var progressTracker = createProgressTracker(
            configuration,
            yensTask
        );

        var yens = Yens.sourceTarget(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return yens.compute();
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(DIJKSTRA, graph.relationshipCount())
        );

        var dijkstra = Dijkstra.singleSource(
            graph,
            configuration.sourceNode(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return dijkstra.compute();
    }

    SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var parameters = configuration.toParameters();
        var progressTracker = createProgressTracker(configuration, Tasks.leaf(SPANNING_TREE));

        var prim = new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker
        );

        return prim.compute();
    }

    SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var mappedSourceNodeId = graph.toMappedNodeId(parameters.sourceNode());
        var mappedTargetNodeIds = parameters.targetNodes().stream()
            .map(graph::safeToMappedNodeId)
            .collect(Collectors.toList());

        var subtasks = new ArrayList<Task>();
        subtasks.add(Tasks.leaf("Traverse", configuration.targetNodes().size()));
        if (configuration.applyRerouting()) {
            var nodeCount = graph.nodeCount();
            subtasks.add(Tasks.leaf("Reroute", nodeCount));
        }
        var progressTracker = createProgressTracker(configuration, Tasks.task(STEINER, subtasks));

        var steiner = new ShortestPathsSteinerAlgorithm(
            graph,
            mappedSourceNodeId,
            mappedTargetNodeIds,
            parameters.delta(),
            parameters.concurrency(),
            parameters.applyRerouting(),
            DefaultPool.INSTANCE,
            progressTracker
        );

        return steiner.compute();
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var traversalTask = Tasks.leaf("Traversal", graph.nodeCount());
        var task = Tasks.task(AlgorithmLabels.TOPOLOGICAL_SORT, List.of(initializationTask, traversalTask));
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = new TopologicalSort(
            graph,
            progressTracker,
            configuration.concurrency(),
            configuration.computeMaxDistanceFromSource()
        );

        return algorithm.compute();
    }

    private MSBFSASPAlgorithm selectAlgorithm(Graph graph, AllShortestPathsConfig configuration) {
        if (configuration.hasRelationshipWeightProperty()) {
            return new WeightedAllShortestPaths(
                graph,
                DefaultPool.INSTANCE,
                configuration.concurrency()
            );
        } else {
            return new MSBFSAllShortestPaths(
                graph,
                configuration.concurrency(),
                DefaultPool.INSTANCE
            );
        }
    }

    private ProgressTracker createProgressTracker(
        AlgoBaseConfig configuration,
        Task progressTask
    ) {
        if (configuration.logProgress()) {
            return new TaskProgressTracker(
                progressTask,
                (org.neo4j.logging.Log) log.getNeo4jLog(),
                configuration.concurrency(),
                configuration.jobId(),
                requestScopedDependencies.getTaskRegistryFactory(),
                requestScopedDependencies.getUserLogRegistryFactory()
            );
        }

        return new TaskTreeProgressTracker(
            progressTask,
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.concurrency(),
            configuration.jobId(),
            requestScopedDependencies.getTaskRegistryFactory(),
            requestScopedDependencies.getUserLogRegistryFactory()
        );
    }
}
