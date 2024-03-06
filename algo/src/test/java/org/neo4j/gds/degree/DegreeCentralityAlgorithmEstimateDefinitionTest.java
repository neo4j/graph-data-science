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
package org.neo4j.gds.degree;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.neo4j.gds.assertions.MemoryEstimationAssert.assertThat;

class DegreeCentralityAlgorithmEstimateDefinitionTest {

    @ParameterizedTest
    @CsvSource({
        "10_000, 48",
        "500_000, 48",
        "10_000_000, 48"
    })
    void testMemoryEstimation(long nodeCount, long expectedMemory) {
        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition(false).memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, 48",
        "10_000, 2, 48",
        "10_000, 128, 48"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrency(long nodeCount, int concurrency, long expectedMemory) {
        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition(false).memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 80_088",
        "500_000, 4_000_088",
        "10_000_000, 80_000_088"
    })
    void testMemoryEstimationWithRelationshipWeight(long nodeCount, long expectedMemory) {
        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition(true).memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, 4)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }

    @ParameterizedTest
    @CsvSource({
        "10_000, 1, 80_088",
        "10_000, 2, 80_088",
        "10_000, 128, 80_088"
    })
    void shouldGiveTheSameEstimationRegardlessOfTheConcurrencyWeighted(long nodeCount, int concurrency, long expectedMemory) {
        var memoryEstimation = new DegreeCentralityAlgorithmEstimateDefinition(true).memoryEstimation();
        assertThat(memoryEstimation)
            .memoryRange(nodeCount, concurrency)
            .hasSameMinAndMaxEqualTo(expectedMemory);
    }
}
