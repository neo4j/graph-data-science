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
import org.neo4j.gds.assertions.MemoryEstimationAssert;

class ApproxMaxKCutMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "nodeCount: {0}, k: {1}")
    @CsvSource(
        {
            "10_000, 2, 200_272",
            "10_000, 5, 440_272",
            "40_000, 2, 800_272",
            "40_000, 5, 1_760_272"
        }
    )
    void memoryEstimationWithVNS(long nodeCount, byte k, long expectedMemory) {
        var estimationParameters = new ApproxMaxKCutMemoryEstimationParameters(k, 4);

        var memoryEstimate = new ApproxMaxKCutMemoryEstimateDefinition(estimationParameters).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest(name = "nodeCount: {0}, k: {1}")
    @CsvSource(
        {
            "10_000, 2, 190_232",
            "40_000, 5, 1_720_232"
        }
    )
    void memoryEstimationWithoutVNS(long nodeCount, byte k, long expectedMemory) {
        var estimationParameters = new ApproxMaxKCutMemoryEstimationParameters(k, 0);

        var memoryEstimate = new ApproxMaxKCutMemoryEstimateDefinition(estimationParameters).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
