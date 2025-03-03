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
package org.neo4j.gds.hdbscan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HDBScanMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource(
        {
            "1_000,     2,  448440",
            "1_000,     10, 704440",
            "1_000_000, 2,  450733600",
            "1_000_000, 10, 706733600"
        }
    )
    void shouldEstimateMemoryCorrectly(long nodeCount, int samples, long expected){

        var params = mock(HDBScanParameters.class);
        when(params.samples()).thenReturn(samples);
        when(params.leafSize()).thenReturn(5L);


        var memoryEstimate  = new HDBScanMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount)
            .hasSameMinAndMaxEqualTo(expected);
    }
    @ParameterizedTest
    @CsvSource(
        {
            "1_000,     2,  587704",
            "1_000,     10, 378808",
            "1_000_000, 2,  593339936",
            "1_000_000, 10, 379430432"

        }
    )
    void shouldEstimateMemoryCorrectlyForDifferentLeafs(long nodeCount, long leafChild, long expected){

        var params = mock(HDBScanParameters.class);
        when(params.samples()).thenReturn(2);
        when(params.leafSize()).thenReturn(leafChild);

        var memoryEstimate  = new HDBScanMemoryEstimateDefinition(params).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimate)
            .memoryRange(nodeCount)
            .hasSameMinAndMaxEqualTo(expected);
    }

    @Test
    void shouldEstimateCorrectlyNumberOfKdNodes(){
        //    10      |  1
        //    5 5     |  2
        //  2 3 2 3   |  4   = 7 (exactly actual for leaf=3)

        assertThat(HDBScanMemoryEstimateDefinition.estimatedNumberOfNodes(10,3)).isEqualTo(7);
        //lef child 2
        //   1 2  1 2 |  8   =  15 ( +4 from actual for leaf=2)
        assertThat(HDBScanMemoryEstimateDefinition.estimatedNumberOfNodes(10,2)).isEqualTo(15);
        //lef child 1
        // 1 1 1 2 1 1 1 2   | 8
        //      1 1      1 1 |   16 (+12 from actual for lef = 1)  //overapproximatiion corrected by 2*nodeCount-1
        assertThat(HDBScanMemoryEstimateDefinition.estimatedNumberOfNodes(10,1)).isEqualTo(19);


    }

}
