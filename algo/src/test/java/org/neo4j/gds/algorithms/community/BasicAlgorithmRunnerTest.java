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
package org.neo4j.gds.algorithms.community;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetric;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetricsService;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.logging.Log;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BasicAlgorithmRunnerTest {

    @Test
    void shouldRegisterAlgorithmMetricCountForSuccess() {
        var graphMock = mock(Graph.class);
        when(graphMock.isEmpty()).thenReturn(false);

        var graphStoreCatalogServiceMock = mock(GraphStoreCatalogService.class);
        when(graphStoreCatalogServiceMock.getGraphWithGraphStore(any(), any(), any(), any(), any()))
            .thenReturn(Pair.of(graphMock, mock(GraphStore.class)));

        var algorithmMetricMock = mock(AlgorithmMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var logMock = mock(Log.class);
        when(logMock.getNeo4jLog()).thenReturn(Neo4jProxy.testLog());

        var runner = new BasicAlgorithmRunner(
            graphStoreCatalogServiceMock,
            TaskRegistryFactory.empty(),
            EmptyUserLogRegistryFactory.INSTANCE,
            mock(AlgorithmMemoryValidationService.class),
            algorithmMetricsServiceMock,
            logMock
        );

        var algorithmMock = mock(Algorithm.class, RETURNS_DEEP_STUBS);
        when(algorithmMock.compute()).thenReturn("WooHoo");
        var algorithmFactoryMock = mock(GraphAlgorithmFactory.class);
        when(algorithmFactoryMock.taskName()).thenReturn("TestingMetrics");
        when(algorithmFactoryMock.build(any(), any(), any(), any(), any())).thenReturn(algorithmMock);

        runner.run(
            "foo",
            mock(AlgoBaseConfig.class),
            Optional.empty(),
            algorithmFactoryMock,
            mock(User.class),
            DatabaseId.EMPTY
        );

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
        var graphMock = mock(Graph.class);
        when(graphMock.isEmpty()).thenReturn(false);

        var graphStoreCatalogServiceMock = mock(GraphStoreCatalogService.class);
        when(graphStoreCatalogServiceMock.getGraphWithGraphStore(any(), any(), any(), any(), any()))
            .thenReturn(Pair.of(graphMock, mock(GraphStore.class)));

        var algorithmMetricMock = mock(AlgorithmMetric.class);
        var algorithmMetricsServiceMock = mock(AlgorithmMetricsService.class);
        when(algorithmMetricsServiceMock.create(anyString())).thenReturn(algorithmMetricMock);

        var logMock = mock(Log.class);
        when(logMock.getNeo4jLog()).thenReturn(Neo4jProxy.testLog());

        var runner = new BasicAlgorithmRunner(
            graphStoreCatalogServiceMock,
            TaskRegistryFactory.empty(),
            EmptyUserLogRegistryFactory.INSTANCE,
            mock(AlgorithmMemoryValidationService.class),
            algorithmMetricsServiceMock,
            logMock
        );

        var algorithmMock = mock(Algorithm.class, RETURNS_DEEP_STUBS);
        when(algorithmMock.compute()).thenThrow(new RuntimeException("Ooops"));

        var algorithmFactoryMock = mock(GraphAlgorithmFactory.class);
        when(algorithmFactoryMock.taskName()).thenReturn("TestingMetrics");
        when(algorithmFactoryMock.build(any(), any(), any(), any(), any())).thenReturn(algorithmMock);

        assertThatException().isThrownBy(
            () -> runner.run(
                "foo",
                mock(AlgoBaseConfig.class),
                Optional.empty(),
                algorithmFactoryMock,
                mock(User.class),
                DatabaseId.EMPTY
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
