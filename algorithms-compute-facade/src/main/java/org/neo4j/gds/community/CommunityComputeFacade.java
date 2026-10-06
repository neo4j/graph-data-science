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
import org.neo4j.gds.kcore.KCoreDecomposition;
import org.neo4j.gds.kcore.KCoreDecompositionParameters;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.Kmeans;
import org.neo4j.gds.kmeans.KmeansContext;
import org.neo4j.gds.kmeans.KmeansParameters;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagation;
import org.neo4j.gds.labelpropagation.LabelPropagationParameters;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.Leiden;
import org.neo4j.gds.leiden.LeidenParameters;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.logging.Log;
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
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.wcc.Wcc;
import org.neo4j.gds.wcc.WccParameters;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CommunityComputeFacade {
    private final Log log;

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    public CommunityComputeFacade(
        Log log,
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.log = log;
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    CompletableFuture<TimedAlgorithmResult<KCoreDecompositionResult>> kCore(
        Graph graph,
        KCoreDecompositionParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(KCoreDecompositionResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.kCore(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new KCoreDecomposition(
            graph,
            parameters.concurrency(),
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<KmeansResult>> kMeans(
        Graph graph,
        KmeansParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(KmeansResult.empty(parameters.k())));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.kMeans(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = Kmeans.createKmeans(
            log,
            graph,
            parameters,
            new KmeansContext(DefaultPool.INSTANCE, progressTracker),
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<LabelPropagationResult>> labelPropagation(
        Graph graph,
        LabelPropagationParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(LabelPropagationResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.labelPropagation(graph.relationshipCount(), parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new LabelPropagation(
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

    CompletableFuture<TimedAlgorithmResult<LocalClusteringCoefficientResult>> lcc(
        Graph graph,
        LocalClusteringCoefficientParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(LocalClusteringCoefficientResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.lcc(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new LocalClusteringCoefficient(
            graph,
            parameters.concurrency(),
            parameters.maxDegree(),
            parameters.seedProperty(),
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<LeidenResult>> leiden(
        Graph graph,
        LeidenParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(LeidenResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            CommunityAlgorithmTasks.leiden(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var seedValues = Optional.ofNullable(parameters.seedProperty())
            .map(seedParameter -> CommunityCompanion.extractSeedingNodePropertyValues(graph, seedParameter))
            .orElse(null);

        var algorithm = new Leiden(
            graph,
            parameters,
            seedValues,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
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
