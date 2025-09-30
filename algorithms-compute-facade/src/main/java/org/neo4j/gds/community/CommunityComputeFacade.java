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
import org.neo4j.gds.algorithms.community.CommunityCompanion;
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
import org.neo4j.gds.k1coloring.K1Coloring;
import org.neo4j.gds.k1coloring.K1ColoringParameters;
import org.neo4j.gds.k1coloring.K1ColoringResult;
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
import org.neo4j.gds.louvain.Louvain;
import org.neo4j.gds.louvain.LouvainParameters;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.LocalClusteringCoefficient;
import org.neo4j.gds.triangle.LocalClusteringCoefficientParameters;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;

import java.util.Optional;
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

    CompletableFuture<TimedAlgorithmResult<K1ColoringResult>> k1Coloring(
        Graph graph,
        K1ColoringParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(K1ColoringResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.k1Coloring(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new K1Coloring(
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
            tasks.kCore(graph),
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
            tasks.kMeans(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = Kmeans.createKmeans(
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
            tasks.labelPropagation(graph,parameters),
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
            tasks.lcc(graph,parameters),
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
            tasks.leiden(graph,parameters),
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
            tasks.louvain(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Louvain(
            graph,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

}
