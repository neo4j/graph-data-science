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
package org.neo4j.gds.labelpropagation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.stream.Stream;

class LabelPropagationMemoryEstimateDefinitionTest {


    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 800_464, 4_994_640),
            Arguments.of(4, 801_544, 17_578_248),
            Arguments.of(42, 815_224, 176_970_616)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        var memoryEstimation = new LabelPropagationMemoryEstimateDefinition().memoryEstimation();
        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(100_000L, new Concurrency(concurrency))
            .hasMin(expectedMinBytes)
            .hasMax(expectedMaxBytes);
    }

    @Test
    void shouldBoundMemEstimationToMaxSupportedDegree() {
        var largeNodeCount = ((long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE);
        var memoryEstimation = new LabelPropagationMemoryEstimateDefinition().memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(largeNodeCount, new Concurrency(1))
            .max()
            .isPositive();
    }
}
