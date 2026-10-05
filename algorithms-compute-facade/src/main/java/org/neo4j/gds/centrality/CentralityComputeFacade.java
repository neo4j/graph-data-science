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
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityParameters;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityParameters;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.indirectExposure.IndirectExposure;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELF;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFResult;
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

    public CompletableFuture<TimedAlgorithmResult<CELFResult>> celf(
        Graph graph,
        CELFParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(CELFResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.CELF(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var celf = new CELF(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            celf::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<ClosenessCentralityResult>> closeness(
        Graph graph,
        ClosenessCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ClosenessCentralityResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.closenessCentrality(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var closeness = ClosenessCentrality.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            closeness::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<DegreeCentralityResult>> degree(
        Graph graph,
        DegreeCentralityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(DegreeCentralityResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.degreeCentrality(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var degree = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.orientation(),
            parameters.hasRelationshipWeightProperty(),
            parameters.minBatchSize(),
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            degree::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<IndirectExposureResult>> indirectExposure(
        Graph graph,
        IndirectExposureConfig configuration,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(IndirectExposureResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CentralityAlgorithmTasks.indirectExposure(graph, configuration),
            jobId,
            configuration.concurrency(),
            logProgress
        );

        var indirectExposure = new IndirectExposure(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            indirectExposure::compute,
            jobId
        );
    }
}
