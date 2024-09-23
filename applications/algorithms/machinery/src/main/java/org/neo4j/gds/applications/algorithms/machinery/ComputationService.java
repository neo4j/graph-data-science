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

import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import java.util.function.Supplier;

/**
 * This encapsulates computing stuff with memory guard and metrics. _Could_ be algorithms, could be something else.
 */
class ComputationService {
    private final Log log;
    private final MemoryGuard memoryGuard;
    private final AlgorithmMetricsService algorithmMetricsService;

    ComputationService(Log log, MemoryGuard memoryGuard, AlgorithmMetricsService algorithmMetricsService) {
        this.log = log;
        this.memoryGuard = memoryGuard;
        this.algorithmMetricsService = algorithmMetricsService;
    }

    <CONFIGURATION extends ConcurrencyConfig, RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeAlgorithm(
        CONFIGURATION configuration,
        GraphResources graphResources,
        Algorithm metadata,
        Supplier<MemoryEstimation> estimationSupplier,
        Computation<RESULT_FROM_ALGORITHM> computation
    ) {
        memoryGuard.assertAlgorithmCanRun(metadata, configuration, graphResources.graph(), estimationSupplier);

        return computeWithMetrics(graphResources, metadata, computation);
    }

    private <RESULT_FROM_ALGORITHM> RESULT_FROM_ALGORITHM computeWithMetrics(
        GraphResources graphResources,
        Algorithm metadata,
        Computation<RESULT_FROM_ALGORITHM> computation
    ) {
        var executionMetric = algorithmMetricsService.create(metadata.labelForProgressTracking);

        try (executionMetric) {
            executionMetric.start();

            return computation.compute(graphResources.graph(), graphResources.graphStore());
        } catch (RuntimeException e) {
            log.warn("computation failed, halting metrics gathering", e);
            executionMetric.failed(e);
            throw e;
        }
    }
}
