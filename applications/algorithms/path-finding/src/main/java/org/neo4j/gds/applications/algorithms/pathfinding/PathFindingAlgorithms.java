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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.dag.longestPath.DagLongestPath;
import org.neo4j.gds.dag.longestPath.LongestPathTask;
import org.neo4j.gds.dag.topologicalsort.TopSortTask;
import org.neo4j.gds.dag.topologicalsort.TopologicalSort;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.degree.DegreeCentralityTask;
import org.neo4j.gds.kspanningtree.KSpanningTree;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeTask;
import org.neo4j.gds.paths.astar.AStar;
import org.neo4j.gds.paths.astar.AStarTask;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaStepping;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.Yens;
import org.neo4j.gds.paths.yens.YensProgressTask;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PCSTFast;
import org.neo4j.gds.pricesteiner.PCSTProgressTrackerTaskCreator;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.Prim;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.spanningtree.SpanningTreeProgressTask;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeProgressTask;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalk;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisits;
import org.neo4j.gds.traversal.RandomWalkProgressTask;

import java.util.ArrayList;
import java.util.Optional;
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
        // no progresstracker to close
        return algorithm.compute();
    }

    public BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = BellmanFordProgressTask.create();
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new BellmanFord(
            graph,
            progressTracker,
            graph.toMappedNodeId(configuration.sourceNode()),
            configuration.trackNegativeCycles(),
            configuration.trackPaths(),
            configuration.concurrency()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            false,
            configuration.concurrency()
        );
    }

    /**
     * Here is an example of how resource management and structure collide.
     * Progress tracker is constructed here for BreadthFirstSearch, then inside it is delegated to BFS.
     * Ergo we apply the progress tracker resource machinery inside.
     * But it is not great innit.
     */
    HugeLongArray breadthFirstSearch(Graph graph, BfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(Tasks.leaf(AlgorithmLabel.BFS.asString()), configuration);

        var algorithm = new BreadthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );
    }

    public PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var iterativeTask = DeltaSteppingProgressTask.create();
        var progressTracker = createProgressTracker(iterativeTask, configuration);
        var algorithm = DeltaStepping.of(graph, configuration, DefaultPool.INSTANCE, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    /**
     * Moar resource shenanigans
     *
     * @see #breadthFirstSearch(org.neo4j.gds.api.Graph, org.neo4j.gds.paths.traverse.BfsBaseConfig)
     */
    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(Tasks.leaf(AlgorithmLabel.DFS.asString()), configuration);

        var algorithm = new DepthFirstSearch();

        return algorithm.compute(
            graph,
            configuration,
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );
    }

    public SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        if (!graph.schema().isUndirected()) {
            throw new IllegalArgumentException(
                "The K-Spanning Tree algorithm works only with undirected graphs. Please orient the edges properly");
        }

        var parameters = configuration.toKSpanningTreeParameters();

        var progressTracker = createProgressTracker(
            KSpanningTreeTask.create(graph.relationshipCount()),
            configuration
        );

        var algorithm = new KSpanningTree(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            parameters.k(),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    PathFindingResult longestPath(Graph graph, AlgoBaseConfig configuration) {
        var task = LongestPathTask.create(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return longestPath(graph, configuration.concurrency(), progressTracker);
    }

    public PathFindingResult longestPath(Graph graph, Concurrency concurrency, ProgressTracker progressTracker) {
        var algorithm = new DagLongestPath(
            graph,
            progressTracker,
            concurrency,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            concurrency
        );
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var task = RandomWalkProgressTask.create(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return randomWalk(graph, configuration, progressTracker);
    }

    public Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration, ProgressTracker progressTracker) {
        var algorithm = RandomWalk.create(
            graph,
            configuration.concurrency(),
            configuration.walkParameters(),
            configuration.sourceNodes(),
            configuration.walkBufferSize(),
            configuration.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            false, // progress tracker released internally
            configuration.concurrency()
        );
    }

    PrizeSteinerTreeResult pcst(Graph graph, PCSTBaseConfig configuration) {
        var task = PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(), graph.relationshipCount());

        var progressTracker = createProgressTracker(task, configuration);
        var prizeProperty = graph.nodeProperties(configuration.prizeProperty());
        var algorithm = new PCSTFast(
            graph,
            (v) -> Math.max(prizeProperty.doubleValue(v), 0),
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    HugeAtomicLongArray randomWalkCountingNodeVisits(Graph graph, RandomWalkBaseConfig configuration) {
        var tasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            tasks.add(DegreeCentralityTask.create(graph));
        }
        Task task;
        if (tasks.isEmpty()) {
            task = Tasks.leaf(AlgorithmLabel.RandomWalk.asString(), graph.nodeCount());
        } else {
            task = Tasks.task(AlgorithmLabel.RandomWalk.asString(), tasks);
        }

        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = RandomWalkCountingNodeVisits.create(
            graph,
            configuration.concurrency(),
            configuration.walkParameters(),
            configuration.sourceNodes(),
            configuration.randomSeed(),
            progressTracker,
            DefaultPool.INSTANCE,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            AStarTask.create(graph.relationshipCount()),
            configuration
        );

        var algorithm = AStar.sourceTarget(
            graph,
            configuration.toParameters(),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            false,
            configuration.concurrency()
        );
    }

    /**
     * This is conceptually very pretty, it speaks to you: call Dijkstra on a graph, return the result.
     * Caller is responsible for looking up the graph, including the (implicit) validation that that involves.
     * And they also get to do result rendering, using details of user request and GraphStore state.
     * Down here though it is just the algorithm.
     */
    PathFindingResult singlePairShortestPathDijkstra(Graph graph, DijkstraSourceTargetsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            Tasks.leaf(AlgorithmLabel.Dijkstra.asString(), graph.relationshipCount()), configuration
        );

        var algorithm = Dijkstra.sourceTarget(
            graph,
            configuration.sourceNode(),
            configuration.targetsList(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            false,
            configuration.concurrency()
        );
    }

    public PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {
        var yensTask = YensProgressTask.create(graph.relationshipCount(), configuration.k());

        var progressTracker = createProgressTracker(
            yensTask, configuration
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
            configuration.toParameters(),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            Tasks.leaf(AlgorithmLabel.SingleSourceDijkstra.asString(), graph.relationshipCount()), configuration
        );

        var algorithm = Dijkstra.singleSource(
            graph,
            configuration.sourceNode(),
            false,
            Optional.empty(),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            false,
            configuration.concurrency()
        );
    }

    public SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var progressTracker = createProgressTracker(
            SpanningTreeProgressTask.create(graph.relationshipCount()),
            configuration
        );

        var algorithm = new Prim(
            graph,
            parameters.objective(),
            graph.toMappedNodeId(parameters.sourceNode()),
            progressTracker,
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var mappedSourceNodeId = graph.toMappedNodeId(parameters.sourceNode());
        var mappedTargetNodeIds = parameters.targetNodes()
            .stream()
            .map(graph::safeToMappedNodeId)
            .toList();

        var progressTracker = createProgressTracker(
            SteinerTreeProgressTask.create(parameters, graph.nodeCount()),
            configuration
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
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var task = TopSortTask.create(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return topologicalSort(graph, configuration, progressTracker);
    }

    public TopologicalSortResult topologicalSort(
        Graph graph,
        TopologicalSortBaseConfig configuration,
        ProgressTracker progressTracker
    ) {
        var algorithm = new TopologicalSort(
            graph,
            progressTracker,
            configuration.concurrency(),
            configuration.computeMaxDistanceFromSource(),
            requestScopedDependencies.terminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    private MSBFSASPAlgorithm selectAlgorithm(Graph graph, AllShortestPathsConfig configuration) {
        if (configuration.hasRelationshipWeightProperty()) {
            return new WeightedAllShortestPaths(
                graph,
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                requestScopedDependencies.terminationFlag()
            );
        } else {
            return new MSBFSAllShortestPaths(
                graph,
                configuration.concurrency(),
                DefaultPool.INSTANCE,
                requestScopedDependencies.terminationFlag()
            );
        }
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
