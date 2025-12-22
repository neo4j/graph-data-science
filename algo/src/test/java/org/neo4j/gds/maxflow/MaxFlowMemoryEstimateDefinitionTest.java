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
            "1_000,     1_000,      238_768",
            "1_000,     10_000,     922_768",
            "1_000_000, 1_000_000,  237_380_040",
            "1_000_000, 10_000_000, 921_391_024"
        }
    )
    void shouldEstimateMemoryWithChangingGraphDimensionsCorrectly(long nodeCount, long  relationshipCount, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,false,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount, relationshipCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,   1,  238_768",
            "1_000,   4,  241_912",
            "100_000, 1,  23_739_256",
            "100_000, 4,  24_039_400"
        }
    )
    void shouldEstimateMemoryWithChangingConcurrencyCorrectly(long nodeAndRelCount, int  concurrency, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(
            1,
            1,
            false,
            false
        ).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,1,1,   238_768",
            "1_000,10,1,  239_344",
            "1_000,1,10,  239_344",
            "1_000,10,10, 239_920"

        }
    )
    void shouldEstimateMemoryWithChangingSinksOrTerminalsCorrectly(long nodeAndRelCount, int  sinks,int terminals, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(sinks,terminals,false,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,false, 238_768",
            "1_000,true,  254_944",
            "10_000,false, 2_375_168",
            "10_000,true,  2_535_344"
        }
    )
    void shouldEstimateMemoryGap(long nodeAndRelCount, boolean useGap, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,useGap,false).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,false, 238_768",
            "1_000,true,  270_880",
            "10_000,false, 2_375_168",
            "10_000,true,  2_695_280"
        }
    )
    void shouldEstimateWithNodeConstraints(long nodeAndRelCount, boolean useProperty, long expected){

        var memoryEstimate  = new MaxFlowMemoryEstimateDefinition(1,1,false,useProperty).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeAndRelCount, nodeAndRelCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

}
