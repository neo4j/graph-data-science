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
package org.neo4j.gds.msbfs;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;

class MSBFSMemoryEstimationTest {

    @ParameterizedTest
    @CsvSource({
        "100,1,264",
        "100,4,360"
    })
    void shouldWorkForPredecessor(long nodeCount, int concurrency, long expectedMemory){
        MemoryEstimationAssert.assertThat(MSBFSMemoryEstimation.MSBFSWithPredecessorStrategy())
            .memoryRange(GraphDimensions.of(nodeCount),new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "100,1,216",
        "100,4,288"
    })
    void shouldWorkForANP(long nodeCount, int concurrency, long expectedMemory){
        MemoryEstimationAssert.assertThat(MSBFSMemoryEstimation.MSBFSWithANPStrategy())
            .memoryRange(GraphDimensions.of(nodeCount),new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
