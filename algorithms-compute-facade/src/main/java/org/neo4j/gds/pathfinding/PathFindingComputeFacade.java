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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.PathFindingAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.paths.dijkstra.DijkstraFactory;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.pricesteiner.PCSTFast;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisits;
import org.neo4j.gds.traversal.RandomWalkParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class PathFindingComputeFacade {

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;
    // This service is what the algorithms use for parallelism.
    private final ExecutorService executorService;

    // Request scope dependencies -- can we move these as method parameters?! 🤔
    private final TerminationFlag terminationFlag;

    public PathFindingComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ExecutorService executorService,
        TerminationFlag terminationFlag,
        ProgressTrackerFactory progressTrackerFactory
    ) {
        this.algorithmCaller = algorithmCaller;
        this.executorService = executorService;
        this.terminationFlag = terminationFlag;
        this.progressTrackerFactory = progressTrackerFactory;
    }

    public CompletableFuture<TimedAlgorithmResult<HugeAtomicLongArray>> randomWalkCountingNodeVisits(
        Graph graph,
        RandomWalkParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(
                HugeAtomicLongArray.of(
                    0,
                    ParalleLongPageCreator.passThrough(parameters.concurrency())
                )
            ));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.randomWalkCountingVisits(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var randomWalkCountingNodeVisits = RandomWalkCountingNodeVisits.create(
            graph,
            parameters,
            progressTracker,
            executorService,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            randomWalkCountingNodeVisits::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PrizeSteinerTreeResult>> pcst(
        Graph graph,
        PCSTParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PrizeSteinerTreeResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.pcst(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var prizeProperty = graph.nodeProperties(parameters.prizeProperty());
        var pcstFast = new PCSTFast(
            graph,
            (v) -> Math.max(prizeProperty.doubleValue(v), 0),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            pcstFast::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<PathFindingResult>> singleSourceShortestPathDijkstra(
        Graph graph,
        DijkstraSingleSourceParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(PathFindingResult.empty()));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            PathFindingAlgorithmTasks.singleSourceDijkstra(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var dijkstra = DijkstraFactory.singleSource(
            graph,
            parameters.sourceNode(),
            false,
            Optional.empty(),
            progressTracker,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            dijkstra::compute,
            jobId
        );
    }
}
