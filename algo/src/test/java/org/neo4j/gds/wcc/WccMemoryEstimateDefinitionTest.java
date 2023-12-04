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
import org.neo4j.gds.core.GraphDimensions;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertions.MemoryRangeAssert.assertThat;

class WccMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "Node count: {0}")
    @CsvSource({
        "0, 64",
        "100, 864",
        "100_000_000_000, 800_122_070_392"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrencyNonIncrementalConfiguration(long nodeCount, long expectedMemoryUsage) {
        var wcc = new WccMemoryEstimateDefinition();
        var configMock = mock(WccBaseConfig.class);
        when(configMock.isIncremental()).thenReturn(false);

        var graphDimensions = GraphDimensions.of(nodeCount);
        assertThat(wcc.memoryEstimation(configMock).estimate(graphDimensions, 1).memoryUsage())
            .isEqualTo(wcc.memoryEstimation(configMock).estimate(graphDimensions, 8).memoryUsage())
            .isEqualTo(wcc.memoryEstimation(configMock).estimate(graphDimensions, 64).memoryUsage())
            .hasMin(expectedMemoryUsage)
            .hasMax(expectedMemoryUsage);

    }

    @ParameterizedTest(name = "Node count: {0}")
    @CsvSource({
        "0, 104",
        "100, 1_704",
        "100_000_000_000, 1_600_244_140_760"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrencyIncrementalConfiguration(long nodeCount, long expectedMemoryUsage) {
        var wcc = new WccMemoryEstimateDefinition();
        var configMock = mock(WccBaseConfig.class);
        when(configMock.isIncremental()).thenReturn(true);

        var graphDimensions = GraphDimensions.of(nodeCount);
        assertThat(wcc.memoryEstimation(configMock).estimate(graphDimensions, 1).memoryUsage())
            .isEqualTo(wcc.memoryEstimation(configMock).estimate(graphDimensions, 8).memoryUsage())
            .isEqualTo(wcc.memoryEstimation(configMock).estimate(graphDimensions, 64).memoryUsage())
            .hasMin(expectedMemoryUsage)
            .hasMax(expectedMemoryUsage);
    }

}
