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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.ExecutionMetric;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricsRecorderTest {
    @Test
    void shouldRecordMetricsAndRunAlgorithmAndCaptureMetrics() {
        var algorithmMetricsService = mock(AlgorithmMetricsService.class);
        var executionMetric = mock(ExecutionMetric.class);
        when(algorithmMetricsService.create("my algorithm")).thenReturn(executionMetric);
        var algorithmTelemetry = mock(Telemetry.class);
        var metricsRecorder = new MetricsRecorder(Log.noOpLog(), algorithmMetricsService, algorithmTelemetry);

        var timingsBuilder = mock(AlgorithmProcessingTimingsBuilder.class);
        var graphId = new GraphId(23);
        var label = new StandardLabel("my algorithm");
        var configuration = mock(AlgoBaseConfig.class);
        var constructAndRun = new DummyConstructAndRun();
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(algorithmTelemetry.runAlgorithmAndLogTelemetry(
            timingsBuilder,
            graphId,
            label,
            configuration,
            constructAndRun,
            graph,
            graphStore
        )).thenReturn("my dummy result");
        var result = metricsRecorder.recordMetricsAndRunAlgorithm(
            timingsBuilder,
            graphId,
            label,
            configuration,
            constructAndRun,
            graph,
            graphStore
        );

        assertEquals("my dummy result", result);

        verify(executionMetric).start();
        verify(executionMetric, never()).failed(any());
    }

    @Test
    void shouldRecordAlgorithmFailures() {
        var algorithmMetricsService = mock(AlgorithmMetricsService.class);
        var executionMetric = mock(ExecutionMetric.class);
        when(algorithmMetricsService.create("my algorithm")).thenReturn(executionMetric);
        var algorithmTelemetry = mock(Telemetry.class);
        var metricsRecorder = new MetricsRecorder(Log.noOpLog(), algorithmMetricsService, algorithmTelemetry);

        var timingsBuilder = mock(AlgorithmProcessingTimingsBuilder.class);
        var graphId = new GraphId(23);
        var label = new StandardLabel("my algorithm");
        var configuration = mock(AlgoBaseConfig.class);
        var constructAndRun = new DummyConstructAndRun();
        var exception = new RuntimeException("c'est la vie");
        var graph = mock(Graph.class);
        var graphStore = mock(GraphStore.class);
        when(algorithmTelemetry.runAlgorithmAndLogTelemetry(
            timingsBuilder,
            graphId,
            label,
            configuration,
            constructAndRun,
            graph,
            graphStore
        )).thenThrow(exception);
        try {
            metricsRecorder.recordMetricsAndRunAlgorithm(
                timingsBuilder,
                graphId,
                label,
                configuration,
                constructAndRun,
                graph,
                graphStore
            );

            fail();
        } catch (RuntimeException ignored) {

        }

        verify(executionMetric).start();
        verify(executionMetric).failed(exception);
    }
}
