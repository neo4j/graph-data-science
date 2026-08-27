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
package org.neo4j.gds.centrality;

import org.neo4j.gds.CentralityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPoints;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.bridges.BridgesParameters;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.CompletableFuture;

public class CentralityComputeFacade {
    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    public CentralityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    public CompletableFuture<TimedAlgorithmResult<ArticulationPointsResult>> articulationPoints(
        Graph graph,
        ArticulationPointsParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ArticulationPointsResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.articulationPoints(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );
        var articulationPoints = ArticulationPoints.create(graph, parameters, progressTracker, terminationFlag);

        return algorithmCaller.run(
            articulationPoints::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<BridgeResult>> bridges(
        Graph graph,
        BridgesParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(BridgeResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.bridges(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var bridges = Bridges
            .create(graph, progressTracker, parameters.computeComponents(), terminationFlag);

        return algorithmCaller.run(
            bridges::compute,
            jobId
        );
    }
}
