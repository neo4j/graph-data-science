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
package org.neo4j.gds.approxmaxkcut;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;

class ApproxMaxKCutAlgorithmEstimationTest {

    @ParameterizedTest(name = "nodeCount: {0}, k: {1}")
    @CsvSource({
        "10_000, 2, 200_264",
        "10_000, 5, 440_264",
        "40_000, 2, 800_264",
        "40_000, 5, 1_760_264"
    })
    void memoryEstimationWithVNS(long nodeCount, byte k, long expectedMemory) {
        var configMock = mock(ApproxMaxKCutBaseConfig.class);

        when(configMock.k()).thenReturn(k);
        when(configMock.vnsMaxNeighborhoodOrder()).thenReturn(4);
        when(configMock.concurrency()).thenReturn(4);

        assertMemoryEstimation(
            () -> new ApproxMaxKCutAlgorithmEstimation().memoryEstimation(configMock),
            nodeCount,
            4,
            MemoryRange.of(expectedMemory)
        );
    }

    @ParameterizedTest(name = "nodeCount: {0}, k: {1}")
    @CsvSource({
        "10_000, 2, 190_224",
        "40_000, 5, 1_720_224"
    })
    void memoryEstimationWithoutVNS(long nodeCount, byte k, long expectedMemory) {
        var configMock = mock(ApproxMaxKCutBaseConfig.class);

        when(configMock.k()).thenReturn(k);
        when(configMock.vnsMaxNeighborhoodOrder()).thenReturn(0);
        when(configMock.concurrency()).thenReturn(4);

        assertMemoryEstimation(
            () -> new ApproxMaxKCutAlgorithmEstimation().memoryEstimation(configMock),
            nodeCount,
            4,
            MemoryRange.of(expectedMemory)
        );
    }

    @ParameterizedTest(name = "Concurrency: {0}")
    @ValueSource(ints = {1, 8, 64})
    void shouldReturnTheSameEstimationRegardlessConcurrency(int concurrency) {
        var configMock = mock(ApproxMaxKCutBaseConfig.class);

        when(configMock.k()).thenReturn((byte) 5);
        when(configMock.vnsMaxNeighborhoodOrder()).thenReturn(0);
        when(configMock.concurrency()).thenReturn(concurrency);

        assertMemoryEstimation(
            () -> new ApproxMaxKCutAlgorithmEstimation().memoryEstimation(configMock),
            10_000,
            4,
            MemoryRange.of(430_224L)
        );
    }

}
