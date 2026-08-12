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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.memory.tracking.TotalMemoryReservationExceededException;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MemoryCheckerTest {
    @Test
    void shouldCheckMemoryThenCheckMemoryAndRunAlgorithm() {
        var wasCalled = new AtomicBoolean();
        var memoryGuard = new MemoryGuardStub(null, wasCalled);
        var metricsRecorder = mock(MetricsRecorder.class);
        var memoryChecker = new MemoryChecker(memoryGuard, metricsRecorder);

        var timingsBuilder = mock(AlgorithmProcessingTimingsBuilder.class);
        var graphResources = mock(GraphResources.class);
        var graph = mock(Graph.class);
        when(graphResources.graph()).thenReturn(graph);
        var graphId = new GraphId(System.identityHashCode(graph));
        var label = new StandardLabel("my algorithm");
        var configuration = mock(AlgoBaseConfig.class);
        var algorithm = new DummyConstructAndRun();
        when(metricsRecorder.recordMetricsAndRunAlgorithm(
            timingsBuilder,
            graphId,
            label,
            configuration,
            algorithm,
            graph
        )).thenReturn("my dummy result");
        var result = memoryChecker.checkMemoryAndRunAlgorithm(
            timingsBuilder,
            graphResources,
            null,
            DimensionTransformer.DISABLED,
            User.DEFAULT,
            label,
            configuration,
            algorithm
        );

        assertEquals("my dummy result", result);
        assertTrue(wasCalled.get());
    }

    @Test
    void shouldNotCheckMemoryAndRunAlgorithmWhenMemoryCheckFails() {
        var memoryGuard = new MemoryGuardStub(
            new TotalMemoryReservationExceededException(
                "my onerous task",
                42000L,
                87000L
            ), null
        );
        var memoryChecker = new MemoryChecker(memoryGuard, null);

        var timingsBuilder = mock(AlgorithmProcessingTimingsBuilder.class);
        var graphResources = mock(GraphResources.class);
        var label = new StandardLabel("my algorithm");
        var configuration = mock(AlgoBaseConfig.class);
        try {
            memoryChecker.checkMemoryAndRunAlgorithm(
                timingsBuilder,
                graphResources,
                null,
                DimensionTransformer.DISABLED,
                User.DEFAULT,
                label,
                configuration,
                null
            );

            fail();
        } catch (IllegalStateException e) {
            assertEquals(
                "Memory required to run my onerous task (42000b) exceeds total available memory (87000b).",
                e.getMessage()
            );
        }
    }
}
