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
            "1_000,     1_000,      214_336",
            "1_000,     10_000,     898_336",
            "1_000_000, 1_000_000,  213_254_736",
            "1_000_000, 10_000_000, 897_265_720"
        }
    )
    void shouldEstimateMemoryWithChangingGraphDimensionsCorrectly(long nodeCount, long  relationshipCount, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount, relationshipCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,   1,  214_336",
            "1_000,   4,  217_504",
            "100_000, 1,  21_326_448",
            "100_000, 4,  21_626_616"
        }
    )
    void shouldEstimateMemoryWithChangingConcurrencyCorrectly(long nodeAndRelCount, int  concurrency, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,1,1,   214_336",
            "1_000,10,1,  214_912",
            "1_000,1,10,  214_912",
            "1_000,10,10, 215_488"

        }
    )
    void shouldEstimateMemoryWithChangingSinksOrTerminalsCorrectly(long nodeAndRelCount, int  sinks,int terminals, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(sinks,terminals,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,false, 214_336",
            "1_000,true,  222_424",
            "10_000,false, 2_133_608",
            "10_000,true,  2_213_696"
        }
    )
    void shouldEstimateMemoryGap(long nodeAndRelCount, boolean useGap, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,useGap).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

}
