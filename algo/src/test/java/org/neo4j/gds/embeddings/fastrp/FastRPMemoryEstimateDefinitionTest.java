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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class FastRPMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "NodeCount: {0}, concurrency: {1}")
    @CsvSource(
        {
            "100, 1, 159_808",
            "100, 8, 159_808",
            "100, 128, 159_808",
            "250_000, 8, 399_001_096",
            "1_000_000, 128, 1_596_003_856"
        }
    )
    void shouldComputeMemoryEstimation(long nodeCount, int concurrency, long expectedMemory) {
        var fastRPMemoryEstimation = new FastRPMemoryEstimateDefinition();
        assertThat(fastRPMemoryEstimation.memoryEstimation(128, 0, 0))
            .memoryRange(nodeCount, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @Test
    void shouldHaveCorrectDescription() {
        var fastRPMemoryEstimation = new FastRPMemoryEstimateDefinition();
        assertThat(fastRPMemoryEstimation.memoryEstimation(1, 2, 3))
            .hasDescription("FastRP");
    }
}
