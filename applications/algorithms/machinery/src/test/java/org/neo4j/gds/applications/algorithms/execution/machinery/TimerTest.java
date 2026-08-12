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

import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TimerTest {
    @Test
    void shouldRunAlgorithmAndRecordTimingAndRecordTime() {
        var timer = new Timer();

        var timeSupplier = mock(LongSupplier.class);
        when(timeSupplier.getAsLong()).thenReturn(23L, 87L);
        var computationTimer = new ComputationTimer(timeSupplier);
        var result = timer.runAlgorithmAndRecordTiming(computationTimer, __ -> 42, mock(Graph.class));

        assertEquals(42, result);
        assertEquals(23L, computationTimer.startTimeInEpochMilliseconds());
        assertEquals(64L, computationTimer.durationInMilliseconds());
    }
}
