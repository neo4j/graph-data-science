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
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

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

    private ProgressTracker createProgressTracker(Task task, AlgoBaseConfig configuration) {
        return progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );
    }
}
