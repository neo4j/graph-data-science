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
package org.neo4j.gds.allshortestpaths;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class AllShortestPathsMemoryEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, false, 280",
        "10_000, 4, false, 544",
        "500_000, 4, false, 27_200",
        "10_000_000, 4, false, 544_000",
        "10_000, 1, true, 1_120_456",
        "10_000, 4, true, 1_120_720",
        "500_000, 4, true, 56_000_720",
        "10_000_000, 4, true, 1_120_000_720"
    })
    void testMemoryEstimation(long nodeCount, int concurrency, boolean weighted, long expectedMemory) {
        var memoryEstimation = new AllShortestPathsMemoryEstimateDefinition(weighted).memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }
} 