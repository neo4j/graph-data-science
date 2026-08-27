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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.conductance.Conductance;
import org.neo4j.gds.conductance.ConductanceParameters;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.modularity.ModularityCalculator;
import org.neo4j.gds.modularity.ModularityParameters;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.TriangleCountParameters;
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.triangle.TriangleStream;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
            CommunityAlgorithmTasks.conductance(graph, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var algorithm = new Conductance(
            progressTracker,
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<ModularityResult>> modularity(
        Graph graph,
        ModularityParameters parameters,
        JobId jobId
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ModularityResult.EMPTY));
        }

        var algorithm = ModularityCalculator.create(
            terminationFlag,
            graph,
            graph.nodeProperties(parameters.communityProperty())::longValue,
            parameters.concurrency()
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }

    CompletableFuture<TimedAlgorithmResult<Stream<TriangleResult>>> triangles(
        Graph graph,
        TriangleCountParameters parameters,
        JobId jobId
    ) {

        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Stream.empty()));
        }

        var algorithm = TriangleStream.create(
            terminationFlag, graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.labelFilter()
        );

        return algorithmCaller.run(
            algorithm::compute,
            jobId
        );
    }
}
