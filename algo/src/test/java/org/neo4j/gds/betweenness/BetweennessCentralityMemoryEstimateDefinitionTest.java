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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class BetweennessCentralityMemoryEstimateDefinitionTest {

    @ParameterizedTest(name = "Concurrency: {0}")
    @CsvSource({
        "1, 6_000_376",
        "4, 21_601_192",
        "42, 219_211_528"
    })
    void testMemoryEstimation(int concurrency, long expectedBytes) {
        var memoryEstimation = new BetweennessCentralityMemoryEstimateDefinition().memoryEstimation(false);
        assertThat(memoryEstimation)
            .memoryRange(100_000L, concurrency)
            .hasSameMinAndMaxEqualTo(expectedBytes);
    }

    @ParameterizedTest(name = "Concurrency: {0}")
    @CsvSource({
        "1, 7_213_000",
        "4, 26_451_688",
        "42, 270_141_736"
    })
    void testMemoryEstimationWithRelationshipWeight(int concurrency, long expectedBytes) {
        var memoryEstimation = new BetweennessCentralityMemoryEstimateDefinition().memoryEstimation(true);
        assertThat(memoryEstimation)
            .memoryRange(100_000L, concurrency)
            .hasSameMinAndMaxEqualTo(expectedBytes);
    }
}
