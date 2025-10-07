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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

class MaxFlowMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,     1_000,      110_288",
            "1_000,     10_000,     794_288",
            "1_000_000, 1_000_000,  109_252_248",
            "1_000_000, 10_000_000, 793_263_232"
        }
    )
    void shouldEstimateMemoryWithChangingGraphDimensionsCorrectly(long nodeCount, long  relationshipCount, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount, relationshipCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,   1,  110_288",
            "1_000,   4,  113_744",
            "100_000, 1,  10_926_160",
            "100_000, 4,  11_226_616"
        }
    )
    void shouldEstimateMemoryWithChangingConcurrencyCorrectly(long nodeAndRelCount, int  concurrency, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,1,1,   110_288",
            "1_000,10,1,  110_864",
            "1_000,1,10,  110_864",
            "1_000,10,10, 111_440"

        }
    )
    void shouldEstimateMemoryWithChangingSinksOrTerminalsCorrectly(long nodeAndRelCount, int  sinks,int terminals, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(sinks,terminals).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

}
