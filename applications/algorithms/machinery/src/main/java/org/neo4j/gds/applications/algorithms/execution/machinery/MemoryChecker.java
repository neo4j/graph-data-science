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

import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuardExceptionTransformer;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.memory.tracking.MemoryGuardException;

import java.util.function.Supplier;

/**
 * Algorithm execution is guarded by a memory check
 */
class MemoryChecker {
    private final MemoryGuard memoryGuard;
    private final MetricsRecorder metricsRecorder;

    MemoryChecker(MemoryGuard memoryGuard, MetricsRecorder metricsRecorder) {
        this.memoryGuard = memoryGuard;
        this.metricsRecorder = metricsRecorder;
    }

    <CONFIGURATION extends AlgoBaseConfig, RESULT> RESULT checkMemoryAndRunAlgorithm(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphResources graphResources,
        Supplier<MemoryEstimation> estimationSupplier,
        DimensionTransformer dimensionTransformer,
        User user,
        Label label,
        CONFIGURATION configuration,
        ConstructAndRun<RESULT> constructAndRun
    ) {
        try {
            memoryGuard.assertAlgorithmCanRun(
                graphResources.graph(),
                graphResources.graphStore(),
                configuration.relationshipTypesFilter(),
                configuration.concurrency(),
                estimationSupplier,
                label,
                dimensionTransformer,
                user,
                configuration.jobId(),
                configuration.sudo()
            );
        } catch (MemoryGuardException e) {
            MemoryGuardExceptionTransformer.throwAsIllegalStateException(e);
        }

        var graphId = GraphId.from(graphResources.graph());

        return metricsRecorder.recordMetricsAndRunAlgorithm(
            timingsBuilder,
            graphId,
            label,
            configuration,
            constructAndRun,
            graphResources.graph()
        );
    }
}
