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
package org.neo4j.gds.harmonic;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.core.concurrency.Concurrency;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class HarmonicCentralityAlgorithmEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, 80_368",
        "10_000, 4, 80_632",
        "500_000, 4, 4_000_632",
        "10_000_000, 4, 80_000_632",
        "10_000, 2, 80_456",
        "10_000, 128, 91_544"
    })
    void testMemoryEstimation(long nodeCount, int concurrency, long expectedMemory) {
        var memoryEstimation = new HarmonicCentralityAlgorithmEstimateDefinition().memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, new Concurrency(concurrency))
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }
}
