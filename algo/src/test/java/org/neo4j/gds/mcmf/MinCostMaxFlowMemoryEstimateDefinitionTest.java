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
package org.neo4j.gds.mcmf;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

class MinCostMaxFlowMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,     1_000,      303_520",
            "1_000,     10_000,     1_059_520",
            "1_000_000, 1_000_000,  301_505_664",
            "1_000_000, 10_000_000, 1_057_516_648"
        }
    )
    void shouldEstimateMemoryWithChangingGraphDimensionsCorrectly(
        long nodeCount,
        long relationshipCount,
        long expected
    ) {

        var memoryEstimate = new MinCostMaxFlowMemoryEstimateDefinition(1, 1,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount, relationshipCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,   1,  303_520",
            "1_000,   4,  306_664",
            "100_000, 1,  30_152_384",
            "100_000, 4,  30_452_528"
        }
    )
    void shouldEstimateMemoryWithChangingConcurrencyCorrectly(long nodeAndRelCount, int concurrency, long expected) {

        var memoryEstimate = new MinCostMaxFlowMemoryEstimateDefinition(1, 1,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,  1,   1,   303_520",
            "1_000,  10,  1,   304_096",
            "1_000,  1,   10,  304_096",
            "1_000,  10,  10,  304_672"

        }
    )
    void shouldEstimateMemoryWithChangingSinksOrTerminalsCorrectly(
        long nodeAndRelCount,
        int sinks,
        int terminals,
        long expected
    ) {

        var memoryEstimate = new MinCostMaxFlowMemoryEstimateDefinition(sinks, terminals,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,false, 303_520",
            "1_000,true,  335_632",
            "10_000,false, 3_017_048",
            "10_000,true,  3_337_160"
        }
    )
    void shouldEstimateWithNodeConstraints(
        long nodeAndRelCount,
        boolean useNodeProperty,
        long expected
    ) {

        var memoryEstimate = new MinCostMaxFlowMemoryEstimateDefinition(1, 1,useNodeProperty).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }
}
