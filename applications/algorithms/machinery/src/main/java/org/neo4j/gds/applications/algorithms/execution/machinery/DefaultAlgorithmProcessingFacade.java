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
import org.neo4j.gds.applications.algorithms.machinery.RequestConfiguration;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedLog;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphLoadETL;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.LongConsumer;
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
        GraphLoadETL graphLoadETL,
        Optional<String> secondRelationshipProperty,
        Optional<GraphLoadETL> secondGraphLoadETL,
        Supplier<MemoryEstimation> estimationSupplier,
        DimensionTransformer dimensionTransformer,
        Label label,
        RequestConfiguration requestConfiguration,
        CONFIGURATION configuration,
        ConstructAndRun<RESULT> constructAndRun,
        Optional<SideEffect<RESULT, METADATA>> sideEffect,
        ResultRenderer<RESULT, TRANSFORMED_RESULT, METADATA> resultRenderer
    ) {
        try (var requestScopedLog = RequestScopedLog.create(log, requestCorrelationId)) {
            var timingsBuilder = new AlgorithmProcessingTimingsBuilder();

            requestScopedLog.onLoadingGraph();
            var graphResources = loadFirstGraph(
                timingsBuilder,
                databaseId,
                graphName,
                user,
                graphParameters,
                relationshipProperty,
                graphLoadETL
            );

            var secondGraphResources = loadSecondGraph(
                timingsBuilder,
                databaseId,
                graphName,
                user,
                graphParameters,
                secondRelationshipProperty,
                secondGraphLoadETL
            );

            return computationQueue.enqueueComputation(
                requestScopedLog,
                timingsBuilder,
                graphResources,
                secondGraphResources,
                estimationSupplier,
                dimensionTransformer,
                user,
                label,
                requestConfiguration,
                configuration,
                constructAndRun,
                sideEffect,
                resultRenderer
            );
        }
    }

    private GraphResources loadFirstGraph(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphLoadETL graphLoadETL
    ) {
        return loadGraph(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            graphLoadETL,
            timingsBuilder::withPreProcessingMillis
        );
    }

    /**
     * @param graphLoadETL indirect parameterisation: if you provide an ETL pipeline, we load your graph;
     *                     even if you provide an empty ETL pipeline...
     */
    private Optional<GraphResources> loadSecondGraph(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        Optional<GraphLoadETL> graphLoadETL
    ) {
        // yeah... indirect parametrisation ftw, or something
        return graphLoadETL.map(loadETL -> loadGraph(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            loadETL,
            timingsBuilder::withSecondPreProcessingMillis
        ));

    }

    private GraphResources loadGraph(
        DatabaseId databaseId,
        GraphName graphName,
        User user,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        GraphLoadETL graphLoadETL,
        LongConsumer timingHook
    ) {
        try (var ignored = ProgressTimer.start(timingHook)) {
            return graphStoreCatalogService.loadGraphResources(
                databaseId,
                graphName,
                user,
                graphParameters,
                relationshipProperty,
                graphLoadETL
            );
        }
    }
}
