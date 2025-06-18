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
        "100,1,0, 3472",
        "100,1,10, 3568",
        "100,4,0, 13720",
        "100,4,10, 13816",
        "200,1,0, 6672",
        "200,1,10, 6768",
        "200,4,0, 26520",
        "201,4,10, 26744"

    })
    void shouldWorkForPredecessor(long nodeCount, int concurrency,  int sourceNodesSize, long expectedMemory){
        MemoryEstimationAssert.assertThat(MSBFSMemoryEstimation.MSBFSWithPredecessorStrategy(sourceNodesSize))
            .memoryRange(GraphDimensions.of(nodeCount),new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "100,1,0, 2632",
        "100,1,10, 2728",
        "100,4,0, 10360",
        "100,4,10, 10456",
        "200,1,0, 5032",
        "200,1,10, 5128",
        "200,4,0, 19960",
        "200,4,10, 20056"
    })
    void shouldWorkForANP(long nodeCount, int concurrency,  int sourceNodesSize, long expectedMemory){
        MemoryEstimationAssert.assertThat(MSBFSMemoryEstimation.MSBFSWithANPStrategy(sourceNodesSize))
            .memoryRange(GraphDimensions.of(nodeCount),new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

}
