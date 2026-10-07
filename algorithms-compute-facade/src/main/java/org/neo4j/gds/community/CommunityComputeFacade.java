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
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.louvain.Louvain;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimization;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationParameters;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.scc.Scc;
import org.neo4j.gds.scc.SccParameters;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccParameters;

import java.util.concurrent.CompletableFuture;

public class CommunityComputeFacade {
    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    public CommunityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    CompletableFuture<TimedAlgorithmResult<LouvainResult>> louvain(
        Graph graph,
        LouvainParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(LouvainResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.louvain(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Louvain(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<ModularityOptimizationResult>> modularityOptimization(
        Graph graph,
        ModularityOptimizationParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ModularityOptimizationResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.modularityOptimization(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var seedPropertyValues = parameters.seedProperty()
            .map(seedProperty -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedProperty))
            .orElse(null);

        var algorithm = new ModularityOptimization(
            graph,
            parameters.maxIterations(),
            parameters.tolerance(),
            seedPropertyValues,
            parameters.concurrency(),
            parameters.batchSize(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<HugeLongArray>> scc(
        Graph graph,
        SccParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(HugeLongArray.newArray(0)));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.scc(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Scc(
            graph,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<DisjointSetStruct>> wcc(
        Graph graph,
        WccParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(new HugeAtomicDisjointSetStruct(
                0,
                new Concurrency(1)
            )));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.wcc(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Wcc(
            graph,
            DefaultPool.INSTANCE,
            ParallelUtil.DEFAULT_BATCH_SIZE,
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
