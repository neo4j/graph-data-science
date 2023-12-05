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
package org.neo4j.gds.wcc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WccMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "Node count: {0}")
    @CsvSource({
        "0, 64",
        "100, 864",
        "100_000_000_000, 800_122_070_392"
    })
    void wccMemoryEstimation(long nodeCount, long expectedMemoryUsage) {
        var configMock = mock(WccBaseConfig.class);
        when(configMock.isIncremental()).thenReturn(false);

        var memoryEstimation = new WccMemoryEstimateDefinition().memoryEstimation(configMock);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
            .hasSameMinAndMaxEqualTo(expectedMemoryUsage);
    }

    @ParameterizedTest(name = "Node count: {0}")
    @CsvSource({
        "0, 104",
        "100, 1_704",
        "100_000_000_000, 1_600_244_140_760"
    })
    void incrementalWccMemoryEstimation(long nodeCount, long expectedMemoryUsage) {
        var configMock = mock(WccBaseConfig.class);
        when(configMock.isIncremental()).thenReturn(true);

        var memoryEstimation = new WccMemoryEstimateDefinition().memoryEstimation(configMock);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
            .hasSameMinAndMaxEqualTo(expectedMemoryUsage);
    }

    @ParameterizedTest(name = "Concurrency: {0}")
    @CsvSource({
        "1, 864",
        "4, 864",
        "128, 864"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrency(int concurrency, long expectedMemory) {
        var configMock = mock(WccBaseConfig.class);

        var memoryEstimation = new WccMemoryEstimateDefinition().memoryEstimation(configMock);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(100, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest(name = "Concurrency: {0}")
    @CsvSource({
        "1, 1_704",
        "4, 1_704",
        "128, 1_704"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrencyIncrementalConfiguration(int concurrency, long expectedMemory) {
        var configMock = mock(WccBaseConfig.class);
        when(configMock.isIncremental()).thenReturn(true);

        var memoryEstimation = new WccMemoryEstimateDefinition().memoryEstimation(configMock);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(100, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }
}
