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
package org.neo4j.gds.mem;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class MemoryGaugeTest {

    @Test
    void canReserveMemory() {
        var memoryGauge = new MemoryGauge(new AtomicLong(19));
        var availableMemoryAfterReservation = memoryGauge.tryToReserveMemory(10);

        assertThat(availableMemoryAfterReservation).isEqualTo(9);
    }

    @Test
    void canReserveAllAvailableMemory() {
        var memoryGauge = new MemoryGauge(new AtomicLong(10));
        var availableMemoryAfterReservation = memoryGauge.tryToReserveMemory(10);

        assertThat(availableMemoryAfterReservation).isEqualTo(0);
    }

    @Test
    void canNotReserveMoreThanAvailableMemory() {
        var memoryGauge = new MemoryGauge(new AtomicLong(99));

        assertThatExceptionOfType(MemoryReservationExceededException.class)
            .isThrownBy(() -> memoryGauge.tryToReserveMemory(119))
            .satisfies(ex -> {
                assertThat(ex.bytesRequired()).isEqualTo(119);
                assertThat(ex.bytesAvailable()).isEqualTo(99);
            });
    }

    @Test
    void canReleaseMemory() {
        var memoryGauge = new MemoryGauge(new AtomicLong(10));
        memoryGauge.tryToReserveMemory(7);

        var memoryAfterRelease = memoryGauge.releaseMemory(3);

        assertThat(memoryAfterRelease).isEqualTo(6);
    }
}
