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
package org.neo4j.gds.community;

import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutParameters;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.cliqueCounting.CliqueCounting;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingParameters;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.hdbscan.HDBScan;
import org.neo4j.gds.hdbscan.HDBScanParameters;
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.CompletableFuture;

public class CommunityComputeFacade {

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    // Local dependencies
    private final CommunityAlgorithmTasks tasks = new CommunityAlgorithmTasks();


    public CommunityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }


    public CompletableFuture<TimedAlgorithmResult<ApproxMaxKCutResult>> approxMaxKCut(
        Graph graph,
        ApproxMaxKCutParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ApproxMaxKCutResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.approximateMaximumKCut(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var approxMaxKCut = ApproxMaxKCut.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            approxMaxKCut::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<CliqueCountingResult>> cliqueCounting(
        Graph graph,
        CliqueCountingParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(CliqueCountingResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.cliqueCounting(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = CliqueCounting.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<ConductanceResult>> conductance(
        Graph graph,
        ConductanceParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ConductanceResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.conductance(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Conductance(
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<Labels>> hdbscan(
        Graph graph,
        HDBScanParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Labels.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.hdbscan(graph),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new HDBScan(
            graph,
            graph.nodeProperties(parameters.nodeProperty()),
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }
}
