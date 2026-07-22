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
package org.neo4j.gds.applications.algorithms.machinery;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.memory.tracking.AvailableMemoryReservationExceededException;
import org.neo4j.gds.memory.tracking.TotalMemoryReservationExceededException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemoryGuardExceptionTransformerTest {

    @Test
    void shouldThrowAvailableMemoryReservationExceededException() {
        var exception = new AvailableMemoryReservationExceededException("WCC", 100, 50);

        assertThatThrownBy(() -> MemoryGuardExceptionTransformer.throwAsIllegalStateException(exception))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Memory required to run WCC (100b) exceeds current available memory (50b).");
    }

    @Test
    void shouldThrowTotalMemoryReservationExceededException() {
        var exception = new TotalMemoryReservationExceededException("Node2Vec", 200, 150);

        assertThatThrownBy(() -> MemoryGuardExceptionTransformer.throwAsIllegalStateException(exception))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Memory required to run Node2Vec (200b) exceeds total available memory (150b).");
    }

}
