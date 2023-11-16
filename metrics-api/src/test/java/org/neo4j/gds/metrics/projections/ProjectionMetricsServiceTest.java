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
package org.neo4j.gds.metrics.projections;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.gds.metrics.ExecutionMetricRegistrar;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class ProjectionMetricsServiceTest {

    @Test
    void shouldCreateNativeProjectionMetric() {
        // given
        var registrarMock = Mockito.mock(ExecutionMetricRegistrar.class);
        var metricsService = new ProjectionMetricsService(registrarMock);

        // when
        metricsService.createNative();

        // then
        verify(registrarMock, times(1)).create("native");
        verifyNoMoreInteractions(registrarMock);
    }

    @Test
    void shouldCreateCypherProjectionMetric() {
        // given
        var registrarMock = Mockito.mock(ExecutionMetricRegistrar.class);
        var metricsService = new ProjectionMetricsService(registrarMock);

        // when
        metricsService.createCypher();

        // then
        verify(registrarMock, times(1)).create("cypher");
        verifyNoMoreInteractions(registrarMock);
    }

    @Test
    void shouldCreateCypherV2ProjectionMetric() {
        // given
        var registrarMock = Mockito.mock(ExecutionMetricRegistrar.class);
        var metricsService = new ProjectionMetricsService(registrarMock);

        // when
        metricsService.createCypherV2();

        // then
        verify(registrarMock, times(1)).create("cypherV2");
        verifyNoMoreInteractions(registrarMock);
    }

    @Test
    void shouldCreateSubGraphProjectionMetric() {
        // given
        var registrarMock = Mockito.mock(ExecutionMetricRegistrar.class);
        var metricsService = new ProjectionMetricsService(registrarMock);

        // when
        metricsService.createSubGraph();

        // then
        verify(registrarMock, times(1)).create("subGraph");
        verifyNoMoreInteractions(registrarMock);
    }

}
