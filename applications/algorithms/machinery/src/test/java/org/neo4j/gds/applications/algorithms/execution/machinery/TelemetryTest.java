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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetryTest {
    @Test
    void shouldRunAlgorithmAndLogTelemetryAndLogTelemetry() {
        var computationTimer = mock(ComputationTimer.class);
        when(computationTimer.startTimeInEpochMilliseconds()).thenReturn(117L);
        when(computationTimer.durationInMilliseconds()).thenReturn(87L);
        var telemetryLogger = mock(TelemetryLogger.class);
        var algorithmTimer = mock(Timer.class);
        var telemetry = new Telemetry(() -> computationTimer, telemetryLogger, algorithmTimer);

        var timingsBuilder = mock(AlgorithmProcessingTimingsBuilder.class);
        var constructAndRun = new DummyConstructAndRun();
        var graph = mock(Graph.class);
        when(
            algorithmTimer.runAlgorithmAndRecordTiming(
                computationTimer,
                constructAndRun,
                graph
            )
        ).thenReturn("my dummy result");
        var configuration = mock(AlgoBaseConfig.class);
        var result = telemetry.runAlgorithmAndLogTelemetry(
            timingsBuilder,
            new GraphId(23),
            () -> "my algorithm",
            configuration,
            constructAndRun,
            graph
        );

        assertEquals("my dummy result", result);

        verify(timingsBuilder).withComputeMillis(87L);
        verify(telemetryLogger).logAlgorithm(23, "my algorithm", configuration, 87L, 117L);
    }
}
