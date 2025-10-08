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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
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
import org.neo4j.gds.dag.longestPath.LongestPathTask;
import org.neo4j.gds.dag.topologicalsort.TopSortTask;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeTask;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.maxflow.MaxFlowTask;
import org.neo4j.gds.paths.RelationshipCountProgressTaskFactory;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BFSProgressTask;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DFSProgressTask;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.YensProgressTask;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PCSTProgressTrackerTaskCreator;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeProgressTask;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisitsProgressTaskFactory;
import org.neo4j.gds.traversal.RandomWalkProgressTask;

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

        return algorithmMachinery.getResult(
            () -> algorithms.allShortestPaths(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public BellmanFordResult bellmanFord(Graph graph, AllShortestPathsBellmanFordBaseConfig configuration) {
        var task = BellmanFordProgressTask.create();
        var progressTracker = createProgressTracker(task, configuration);


        return algorithmMachinery.getResultWithoutReleasingProgressTracker(
            () -> algorithms.bellmanFord(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE
            ),
            progressTracker,
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
        var progressTracker = createProgressTracker(BFSProgressTask.create(), configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.breadthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public PathFindingResult deltaStepping(Graph graph, AllShortestPathsDeltaBaseConfig configuration) {
        var progressTracker = createProgressTracker(DeltaSteppingProgressTask.create(), configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.deltaStepping(
                graph,
                configuration.toParameters(),
                progressTracker,
                DefaultPool.INSTANCE
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    /**
     * Moar resource shenanigans
     *
     * @see #breadthFirstSearch(org.neo4j.gds.api.Graph, org.neo4j.gds.paths.traverse.BfsBaseConfig)
     */
    HugeLongArray depthFirstSearch(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(DFSProgressTask.create(), configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.depthFirstSearch(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public SpanningTree kSpanningTree(Graph graph, KSpanningTreeBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            KSpanningTreeTask.create(graph.relationshipCount()),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.kSpanningTree(
                graph,
                configuration.toKSpanningTreeParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    PathFindingResult longestPath(Graph graph, DagLongestPathBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            LongestPathTask.create(graph.nodeCount()),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.longestPath(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    FlowResult maxFlow(Graph graph, MaxFlowBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            MaxFlowTask.create(),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.maxFlow(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    Stream<long[]> randomWalk(Graph graph, RandomWalkBaseConfig configuration) {
        var task = RandomWalkProgressTask.create(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithmMachinery.getResultWithoutReleasingProgressTracker(
            () -> algorithms.randomWalk(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    HugeAtomicLongArray randomWalkCountingNodeVisits(Graph graph, RandomWalkBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            RandomWalkCountingNodeVisitsProgressTaskFactory.create(graph),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.randomWalkCountingNodeVisits(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    PrizeSteinerTreeResult pcst(Graph graph, PCSTBaseConfig configuration) {

        var progressTracker = createProgressTracker(
            PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(), graph.relationshipCount()),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.pcst(graph, configuration.toParameters(), progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }

    public PathFindingResult singlePairShortestPathAStar(Graph graph, ShortestPathAStarBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.AStar, graph.relationshipCount()),
            configuration
        );

        return algorithmMachinery.getResultWithoutReleasingProgressTracker(
            () -> algorithms.singlePairShortestPathAStar(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    PathFindingResult singlePairShortestPathDijkstra(Graph graph, DijkstraSourceTargetsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.Dijkstra, graph.relationshipCount()),
            configuration
        );

        return algorithmMachinery.getResultWithoutReleasingProgressTracker(
            () -> algorithms.singlePairShortestPathDijkstra(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );

    }

    public PathFindingResult singlePairShortestPathYens(Graph graph, ShortestPathYensBaseConfig configuration) {

        var progressTracker = createProgressTracker(
            YensProgressTask.create(
                graph.relationshipCount(),
                configuration.k()
            ), configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.singlePairShortestPathYens(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ), progressTracker,
            configuration.concurrency()
        );
    }

    PathFindingResult singleSourceShortestPathDijkstra(Graph graph, DijkstraBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.SingleSourceDijkstra, graph.relationshipCount()),
            configuration
        );

        return algorithmMachinery.getResultWithoutReleasingProgressTracker(
            () -> algorithms.singleSourceShortestPathDijkstra(
                graph,
                configuration.sourceNode(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public SpanningTree spanningTree(Graph graph, SpanningTreeBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            RelationshipCountProgressTaskFactory.create(AlgorithmLabel.SpanningTree, graph.relationshipCount()),
            configuration
        );
        return algorithmMachinery.getResult(
            () -> algorithms.spanningTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public SteinerTreeResult steinerTree(Graph graph, SteinerTreeBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var progressTracker = createProgressTracker(
            SteinerTreeProgressTask.create(parameters, graph.nodeCount()),
            configuration
        );

        return algorithmMachinery.getResult(
            () -> algorithms.steinerTree(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag(),
                DefaultPool.INSTANCE
            ),
            progressTracker,
            configuration.concurrency()
        );
    }

    public TopologicalSortResult topologicalSort(Graph graph, TopologicalSortBaseConfig configuration) {
        var task = TopSortTask.create(graph);
        var progressTracker = createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> algorithms.topologicalSort(
                graph,
                configuration.toParameters(),
                progressTracker,
                requestScopedDependencies.terminationFlag()
            ),
            progressTracker,
            configuration.concurrency()
        );
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
