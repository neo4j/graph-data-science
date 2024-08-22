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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.stream.Stream;

class PageRankMemoryEstimateDefinitionTest {


    private static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 2412832L),
            Arguments.of(4, 2413000L),
            Arguments.of(42, 2415128L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedBytes) {
        var nodeCount = 100_000;
        var relationshipCount = nodeCount * 10;

        var memoryEstimation = new PageRankMemoryEstimateDefinition().memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount, relationshipCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedBytes);
    }

    @Test
    void shouldComputeMemoryEstimationFor10BElements() {
        var nodeCount = 10_000_000_000L;
        var relationshipCount = 10_000_000_000L;

        var memoryEstimation = new PageRankMemoryEstimateDefinition().memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount, relationshipCount, new Concurrency(4))
            .hasSameMinAndMaxEqualTo(241_286_621_640L);
    }

}
