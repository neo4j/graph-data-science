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
package org.neo4j.gds.k1coloring;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import static org.neo4j.gds.assertions.MemoryRangeAssert.assertThat;

class K1ColoringMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "Concurrency: {0}")
    @CsvSource(value = {
        "1, 825240",
        "4, 862992",
        "42, 1341184"
    })
    void shouldComputeMemoryEstimation(int concurrency, long expectedMemory) {
        long nodeCount = 100_000L;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        K1ColoringStreamConfig config = ImmutableK1ColoringStreamConfig.builder().build();
        final MemoryRange actual = new K1ColoringAlgorithmFactory<>()
            .memoryEstimation(config)
            .estimate(dimensions, concurrency)
            .memoryUsage();

        assertThat(actual)
            .hasSameMinAndMax()
            .hasMin(expectedMemory);
    }

}
