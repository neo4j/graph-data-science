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
package org.neo4j.gds.influenceMaximization;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class CELFMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource(
        {
            "1, 1, 3_280",
            "1, 4, 4_864",
            "1, 64, 36_544",
            "10, 1, 3_528",
            "10, 4, 5_112",
            "10, 64, 36_792",
        }
    )
    void memoryEstimation(int seedSetSize, int concurrency, long expectedMemory) {
        var parameters = mock(CELFParameters.class);
        when(parameters.seedSetSize()).thenReturn(seedSetSize);
        when(parameters.concurrency()).thenReturn(concurrency);

        var memoryEstimation = new CELFMemoryEstimateDefinition(parameters).memoryEstimation();

        assertThat(memoryEstimation)
            .memoryRange(42, 1337, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
