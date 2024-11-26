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
import org.neo4j.gds.allshortestpaths.MSBFSASPAlgorithm;
import org.neo4j.gds.allshortestpaths.MSBFSAllShortestPaths;
import org.neo4j.gds.allshortestpaths.WeightedAllShortestPaths;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.pathfinding.traverse.BreadthFirstSearch;
import org.neo4j.gds.applications.algorithms.pathfinding.traverse.DepthFirstSearch;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
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
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PCSTFast;
import org.neo4j.gds.pricesteiner.PCSTProgressTrackerTaskCreator;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisits;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    // request scoped parameters
    private final RequestScopedDependencies requestScopedDependencies;
    private final ProgressTrackerCreator progressTrackerCreator;

    public PathFindingAlgorithms(
        RequestScopedDependencies requestScopedDependencies,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.requestScopedDependencies = requestScopedDependencies;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    Stream<AllShortestPathsStreamResult> allShortestPaths(Graph graph, AllShortestPathsConfig configuration) {
        var algorithm = selectAlgorithm(graph, configuration);

        return algorithm.compute();
    }

    BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = Tasks.iterativeOpen(
            AlgorithmLabel.BellmanFord.asString(),
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

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    /**
     * Here is an example of how resource management and structure collide.
     * Progress tracker is constructed here for BreadthFirstSearch, then inside it is delegated to BFS.
     * Ergo we apply the progress tracker resource machinery inside.
     * But it is not great innit.
     */
    HugeLongArray breadthFirstSearch(Graph graph, BfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(configuration, Tasks.leaf(AlgorithmLabel.BFS.asString()));

        var algorithm = new BreadthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );
    }

    public PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var iterativeTask = Tasks.iterativeOpen(
            AlgorithmLabel.DeltaStepping.asString(),
            () -> List.of(
                Tasks.leaf(DeltaStepping.Phase.RELAX.name()),
                Tasks.leaf(DeltaStepping.Phase.SYNC.name())
            )
        );
        var progressTracker = createProgressTracker(configuration, iterativeTask);
        var algorithm = DeltaStepping.of(graph, configuration, DefaultPool.INSTANCE, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    /**
     * Moar resource shenanigans
     *
     * @see #breadthFirstSearch(org.neo4j.gds.api.Graph, org.neo4j.gds.paths.traverse.BfsBaseConfig)
     */
    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(configuration, Tasks.leaf(AlgorithmLabel.DFS.asString()));

        var algorithm = new DepthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );
    }

    public SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The K-Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var parameters = configuration.toKSpanningTreeParameters();

        var progressTracker = createProgressTracker(configuration, Tasks.task(
            AlgorithmLabel.KSpanningTree.asString(),
            Tasks.leaf(AlgorithmLabel.SpanningTree.asString(), graph.relationshipCount()),
            Tasks.leaf("Remove relationships")
        ));

        var algorithm = new KSpanningTree(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.k(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    PathFindingResult longestPath(Graph graph, AlgoBaseConfig configuration) {
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var traversalTask = Tasks.leaf("Traversal", graph.nodeCount());
        var task = Tasks.task(AlgorithmLabel.LongestPath.asString(), List.of(initializationTask, traversalTask));
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = new DagLongestPath(
            graph,
            progressTracker,
            configuration.concurrency(),
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            tasks.add(DegreeCentralityFactory.degreeCentralityProgressTask(graph));
        }
        tasks.add(Tasks.leaf("create walks", graph.nodeCount()));
        var task = Tasks.task(AlgorithmLabel.RandomWalk.asString(), tasks);
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = RandomWalk.create(
            graph,
            configuration.concurrency(),
            configuration.walkParameters(),
            configuration.sourceNodes(),
            configuration.walkBufferSize(),
            configuration.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    PrizeSteinerTreeResult pcst(Graph graph, PCSTBaseConfig configuration) {
        var task = PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(),graph.relationshipCount());

        var progressTracker = createProgressTracker(configuration, task);
        var prizeProperty = graph.nodeProperties(configuration.prizeProperty());
        var algorithm = new PCSTFast(
            graph,
            (v) -> Math.max(prizeProperty.longValue(v),0),
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }
    HugeAtomicLongArray randomWalkCountingNodeVisits(Graph graph, RandomWalkBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            tasks.add(DegreeCentralityFactory.degreeCentralityProgressTask(graph));
        }
        Task task;
        if (tasks.isEmpty()) {
            task = Tasks.leaf(AlgorithmLabel.RandomWalk.asString(), graph.nodeCount());
        } else {
            task = Tasks.task(AlgorithmLabel.RandomWalk.asString(), tasks);
        }

        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = RandomWalkCountingNodeVisits.create(
            graph,
            configuration.concurrency(),
            configuration.walkParameters(),
            configuration.sourceNodes(),
            configuration.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    public PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(AlgorithmLabel.AStar.asString(), graph.relationshipCount())
        );

        var algorithm = AStar.sourceTarget(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
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
            Tasks.leaf(AlgorithmLabel.Dijkstra.asString(), graph.relationshipCount())
        );

        var algorithm = Dijkstra.sourceTarget(
            graph,
            configuration.sourceNode(),
            configuration.targetsList(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    public PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {
        var initialTask = Tasks.leaf(AlgorithmLabel.Dijkstra.asString(), graph.relationshipCount());
        var pathGrowingTask = Tasks.leaf("Path growing", configuration.k() - 1);
        var yensTask = Tasks.task(AlgorithmLabel.Yens.asString(), initialTask, pathGrowingTask);

        var progressTracker = createProgressTracker(
            configuration,
            yensTask
        );

        return singlePairShortestPathYens(graph, configuration, progressTracker);
    }

    public PathFindingResult singlePairShortestPathYens(
        Graph graph,
        ShortestPathYensBaseConfig configuration,
        ProgressTracker progressTracker
    ) {
        var algorithm = Yens.sourceTarget(
            graph,
            configuration,
            configuration.concurrency(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(AlgorithmLabel.SingleSourceDijkstra.asString(), graph.relationshipCount())
        );

        var algorithm = Dijkstra.singleSource(
            graph,
            configuration.sourceNode(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, false);
    }

    public SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var parameters = configuration.toParameters();
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.leaf(AlgorithmLabel.SpanningTree.asString(), graph.relationshipCount())
        );

        var algorithm = new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    public SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
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
        var progressTracker = createProgressTracker(
            configuration,
            Tasks.task(AlgorithmLabel.SteinerTree.asString(), subtasks)
        );

        var algorithm = new ShortestPathsSteinerAlgorithm(
            graph,
            mappedSourceNodeId,
            mappedTargetNodeIds,
            parameters.delta(),
            parameters.concurrency(),
            parameters.applyRerouting(),
            DefaultPool.INSTANCE,
            progressTracker,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var initializationTask = Tasks.leaf("Initialization", graph.nodeCount());
        var traversalTask = Tasks.leaf("Traversal", graph.nodeCount());
        var task = Tasks.task(
            AlgorithmLabel.TopologicalSort.asString(),
            List.of(initializationTask, traversalTask)
        );
        var progressTracker = createProgressTracker(configuration, task);

        var algorithm = new TopologicalSort(
            graph,
            progressTracker,
            configuration.concurrency(),
            configuration.computeMaxDistanceFromSource(),
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    private MSBFSASPAlgorithm selectAlgorithm(Graph graph, AllShortestPathsConfig configuration) {
        if (configuration.hasRelationshipWeightProperty()) {
            return new WeightedAllShortestPaths(
                graph,
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                requestScopedDependencies.getTerminationFlag()
            );
        } else {
            return new MSBFSAllShortestPaths(
                graph,
                configuration.concurrency(),
                DefaultPool.INSTANCE,
                requestScopedDependencies.getTerminationFlag()
            );
        }
    }

    private ProgressTracker createProgressTracker(
        AlgoBaseConfig configuration,
        Task task
    ) {
        return progressTrackerCreator.createProgressTracker(configuration, task);
    }
}
