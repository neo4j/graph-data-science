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
package org.neo4j.gds.algorithms.runner;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetric;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetricsService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.logging.Log;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class AlgorithmRunnerTest {


    @Test
    void shouldRegisterAlgorithmMetricCountForSuccess() {
        var algorithmMetricMock = mock(AlgorithmMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var runner = new AlgorithmRunner(
            null,
            null,
            null,
            null,
            null,
            algorithmMetricsServiceMock,
            null
        );

        var algorithmMock = mock(Algorithm.class);
        when(algorithmMock.compute()).thenReturn("WooHoo");


        runner.runAlgorithm(algorithmMock, "TestingMetrics");

        verify(algorithmMetricsServiceMock, times(1)).create("TestingMetrics");
        verify(algorithmMetricMock, times(1)).start();
        verify(algorithmMetricMock, times(1)).close();
        verify(algorithmMetricMock, times(0)).failed();
        verifyNoMoreInteractions(
            algorithmMetricsServiceMock,
            algorithmMetricMock
        );
    }


    @Test
    void shouldRegisterAlgorithmMetricCountForFailure() {
        var algorithmMetricMock = mock(AlgorithmMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var logMock = mock(Log.class);
        when(logMock.getNeo4jLog()).thenReturn(Neo4jProxy.testLog());

        var runner = new AlgorithmRunner(
            logMock,
            null,
            null,
            null,
            null,
            algorithmMetricsServiceMock,
            null
        );

        var algorithmMock = mock(Algorithm.class, RETURNS_DEEP_STUBS);
        when(algorithmMock.compute()).thenThrow(new RuntimeException("Ooops"));

        assertThatException().isThrownBy(
            () -> runner.runAlgorithm(
                algorithmMock,
                "TestingMetrics"
            )
        ).withMessage("Ooops");

        verify(algorithmMetricsServiceMock, times(1)).create("TestingMetrics");
        verify(algorithmMetricMock, times(1)).start();
        verify(algorithmMetricMock, times(1)).close();
        verify(algorithmMetricMock, times(1)).failed();
        verifyNoMoreInteractions(
            algorithmMetricsServiceMock,
            algorithmMetricMock
        );
    }

}
