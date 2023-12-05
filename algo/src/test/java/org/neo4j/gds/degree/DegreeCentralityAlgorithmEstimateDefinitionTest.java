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
package org.neo4j.gds.degree;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class DegreeCentralityAlgorithmEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource({
        "10_000, 32",
        "500_000, 32",
        "10_000_000, 32"
    })
    void testMemoryEstimation(long nodeCount, long expectedMemory) {
        var configurationMock = mock(DegreeCentralityConfig.class);

        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition().memoryEstimation(configurationMock);
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
                .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, 32",
        "10_000, 2, 32",
        "10_000, 128, 32"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrency(long nodeCount, int concurrency, long expectedMemory) {
        var configurationMock = mock(DegreeCentralityConfig.class);

        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition().memoryEstimation(configurationMock);
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 80_072",
        "500_000, 4_000_072",
        "10_000_000, 80_000_072"
    })
    void testMemoryEstimationWithRelationshipWeight(long nodeCount, long expectedMemory) {
        var configurationMock = mock(DegreeCentralityConfig.class);
        when(configurationMock.hasRelationshipWeightProperty()).thenReturn(true);

        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition().memoryEstimation(configurationMock);
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, 80_072",
        "10_000, 2, 80_072",
        "10_000, 128, 80_072"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrencyWeighted(long nodeCount, int concurrency, long expectedMemory) {
        var configurationMock = mock(DegreeCentralityConfig.class);
        when(configurationMock.hasRelationshipWeightProperty()).thenReturn(true);

        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition().memoryEstimation(configurationMock);
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
