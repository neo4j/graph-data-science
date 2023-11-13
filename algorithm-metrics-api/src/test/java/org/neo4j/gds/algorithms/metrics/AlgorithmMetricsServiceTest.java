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
package org.neo4j.gds.algorithms.metrics;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class AlgorithmMetricsServiceTest {

    @Test
    void shouldRegisterStarted() {
        // given
        var registrarMock = mock(AlgorithmMetricRegistrar.class);
        var metricsService = new AlgorithmMetricsService(registrarMock);

        // when
        metricsService.started("foo");

        // then
        verify(registrarMock, times(1)).started("foo");
        verifyNoMoreInteractions(registrarMock);
    }

    @Test
    void shouldRegisterFailed() {
        // given
        var registrarMock = mock(AlgorithmMetricRegistrar.class);
        var metricsService = new AlgorithmMetricsService(registrarMock);

        // when
        metricsService.failed("foo");

        // then
        verify(registrarMock, times(1)).failed("foo");
        verifyNoMoreInteractions(registrarMock);
    }
}
