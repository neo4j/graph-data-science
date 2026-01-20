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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;

import java.util.function.Supplier;

/**
 * This encapsulates computing stuff with memory guard and metrics. _Could_ be algorithms, could be something else.
 */
class ComputationService {
    private final Log log;
    private final MemoryGuard memoryGuard;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final TelemetryLogger telemetryLogger;
    private final String username;

    ComputationService(
        String username,
        Log log,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService, TelemetryLogger telemetryLogger
    ) {
        this.log = log;
        this.memoryGuard = memoryGuard;
        this.algorithmMetricsService = algorithmMetricsService;
        this.username = username;
        this.telemetryLogger = telemetryLogger;
    }

    <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeAlgorithm(
        CONFIGURATION configuration,
        GraphResources graphResources,
        Label label,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation,
        DimensionTransformer dimensionTransformer
    ) {
        memoryGuard.assertAlgorithmCanRun(
            graphResources.graph(),
            graphResources.graphStore(),
            configuration.relationshipTypesFilter(),
            configuration.concurrency(),
            estimationSupplier,
            label,
            dimensionTransformer,
            username,
            configuration.jobId(),
            configuration.sudo()
        );

        return computeWithMetrics(configuration, graphResources, label, computation);
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithMetrics(
        CONFIGURATION configuration,
        GraphResources graphResources,
        Label label,
        Computation<RESULT_FROM_ALGORITHM> computation
    ) {
        var executionMetric = algorithmMetricsService.create(label.asString());

        try (executionMetric) {
            executionMetric.start();
            var timer = ProgressTimer.start();

            var result = computation.compute(graphResources.graph(), graphResources.graphStore());

            timer.stop();

            var graphIdentifier = System.identityHashCode(graphResources.graphStore());

            telemetryLogger.logAlgorithm(graphIdentifier, label.asString(), configuration, timer.getDuration());

            return result;
        } catch (RuntimeException e) {
            log.warn("computation failed, halting metrics gathering", e);
            executionMetric.failed(e);
            throw e;
        }
    }
}
