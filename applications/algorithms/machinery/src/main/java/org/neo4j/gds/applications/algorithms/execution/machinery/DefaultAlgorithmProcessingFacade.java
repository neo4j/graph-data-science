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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedLog;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PostLoadETLHook;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.GraphValidation;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public final class DefaultAlgorithmProcessingFacade implements AlgorithmProcessingFacade {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final ComputationQueue computationQueue;

    private DefaultAlgorithmProcessingFacade(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ComputationQueue computationQueue
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computationQueue = computationQueue;
    }

    public static DefaultAlgorithmProcessingFacade create(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        ExecutorService executorService,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService,
        TelemetryLogger telemetryLogger
    ) {
        var telemetry = Telemetry.create(telemetryLogger);
        var metricsRecorder = new MetricsRecorder(log, algorithmMetricsService, telemetry);
        var memoryChecker = new MemoryChecker(memoryGuard, metricsRecorder);
        var computationFacade = ComputationFacade.create(memoryChecker);
        var computationQueue = new ComputationQueue(executorService, computationFacade);

        return new DefaultAlgorithmProcessingFacade(log, graphStoreCatalogService, computationQueue);
    }

    @Override
    public <CONFIGURATION extends AlgoBaseConfig, RESULT, METADATA, TRANSFORMED_RESULT> CompletableFuture<TRANSFORMED_RESULT> loadGraphThenRunAlgorithm(
        DatabaseId databaseId,
        GraphName graphName,
        RequestCorrelationId requestCorrelationId,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        Optional<Iterable<PostLoadETLHook>> postLoadETLHooks,
        Optional<GraphValidation> graphValidation,
        ConstructAndRun<RESULT> constructAndRun,
        CONFIGURATION configuration,
        TerminationFlag terminationFlag,
        DimensionTransformer dimensionTransformer,
        Supplier<MemoryEstimation> estimationSupplier,
        Label label,
        Optional<SideEffect<RESULT, METADATA>> sideEffect,
        ResultRenderer<RESULT, TRANSFORMED_RESULT, METADATA> resultRenderer
    ) {
        try (var requestScopedLog = RequestScopedLog.create(log, requestCorrelationId)) {
            var timingsBuilder = new AlgorithmProcessingTimingsBuilder();

            requestScopedLog.onLoadingGraph();
            var graphResources = loadGraph(
                timingsBuilder,
                databaseId,
                graphName,
                user,
                graphParameters,
                relationshipProperty,
                graphStoreValidation,
                postLoadETLHooks,
                graphValidation
            );

            return computationQueue.enqueueComputation(
                requestScopedLog,
                timingsBuilder,
                graphResources,
                user,
                constructAndRun,
                configuration,
                dimensionTransformer,
                estimationSupplier,
                label,
                sideEffect,
                resultRenderer
            );
        }
    }

    private GraphResources loadGraph(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphStoreValidation graphStoreValidation,
        Optional<Iterable<PostLoadETLHook>> postLoadETLHooks,
        Optional<GraphValidation> graphValidation
    ) {
        try (var ignored = ProgressTimer.start(timingsBuilder::withPreProcessingMillis)) {
            return graphStoreCatalogService.loadGraphResources(
                databaseId,
                graphName,
                user,
                graphParameters,
                Optional.empty(),
                relationshipProperty,
                graphStoreValidation,
                postLoadETLHooks,
                graphValidation
            );
        }
    }
}
