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
            "1_000,     1_000,      295_472",
            "1_000,     10_000,     1_051_472",
            "1_000_000, 1_000_000,  293_505_616",
            "1_000_000, 10_000_000, 1_049_516_600"
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
            "1_000,   1,  295_472",
            "1_000,   4,  298_616",
            "100_000, 1,  29_352_336",
            "100_000, 4,  29_652_480"
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
            "1_000,  1,   1,   295_472",
            "1_000,  10,  1,   296_048",
            "1_000,  1,   10,  296_048",
            "1_000,  10,  10,  296_624"

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
            "1_000,false, 295_472",
            "1_000,true,  327_584",
            "10_000,false, 2_937_000",
            "10_000,true,  3_257_112"
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
