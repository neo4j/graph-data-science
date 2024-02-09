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
package org.neo4j.gds.paths.dijkstra;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;

import java.util.stream.Stream;

class DijkstraMemoryEstimateDefinitionTest {

    private static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            // trackRelationships = false
            Arguments.of(1_000, false, 40_616),
            Arguments.of(1_000_000, false, 40_125_488L),
            Arguments.of(1_000_000_000, false, 40_131_104_128L),
            // trackRelationships = true
            Arguments.of(1_000, true, 56_832L),
            Arguments.of(1_000_000, true, 56_125_704, 56_125_704L),
            Arguments.of(1_000_000_000, true, 56_133_545_800L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int nodeCount, boolean trackRelationships, long expectedBytes) {

        MemoryEstimationAssert.assertThat(DijkstraMemoryEstimateDefinition.memoryEstimation(trackRelationships))
            .memoryRange(nodeCount,1)
            .hasSameMinAndMaxEqualTo(expectedBytes);
    }

    @Test
    void shouldWorkWithBitset() {
        MemoryEstimationAssert.assertThat(DijkstraMemoryEstimateDefinition.memoryEstimation(false, true))
            .memoryRange(1_000, 1)
            .hasSameMinAndMaxEqualTo(40_616 + 168);
    }


}
