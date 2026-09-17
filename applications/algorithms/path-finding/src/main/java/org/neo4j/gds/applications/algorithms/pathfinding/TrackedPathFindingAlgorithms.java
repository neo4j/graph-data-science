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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.dag.longestPath.DagLongestPathBaseConfig;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tracking.ProgressTracker;

import java.util.stream.Stream;

public class TrackedPathFindingAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final PathFindingAlgorithms algorithms;

    // request scoped parameters
    private final RequestScopedDependencies requestScopedDependencies;
    private final ProgressTrackerCreator progressTrackerCreator;

    public TrackedPathFindingAlgorithms(
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

    HugeLongArray bfs(Graph graph, BfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.bfs(configuration.concurrency()),
            configuration
        );

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

    HugeLongArray dfs(Graph graph, DfsBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.dfs(configuration.concurrency()),
            configuration
        );

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

    PathFindingResult longestPath(Graph graph, DagLongestPathBaseConfig configuration) {
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.longestPath(graph, configuration.concurrency()),
            configuration
        );

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
        var progressTracker = createProgressTracker(
            PathFindingAlgorithmTasks.maxFlow(configuration.concurrency()),
            configuration
        );

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

    private ProgressTracker createProgressTracker(Task task, AlgoBaseConfig configuration) {
        return progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );
    }
}
