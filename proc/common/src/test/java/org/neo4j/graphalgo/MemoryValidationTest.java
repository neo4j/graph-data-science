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
package org.neo4j.graphalgo;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class MemoryValidationTest {

    private static final GraphDimensions TEST_DIMENSIONS = ImmutableGraphDimensions
        .builder()
        .nodeCount(100)
        .maxRelCount(1000)
        .build();

    static Stream<Arguments> input() {
        var fixedMemory = MemoryEstimations.builder().fixed("foobar", 1337);
        var memoryRange = MemoryEstimations
            .builder()
            .rangePerGraphDimension("foobar", (dimensions, concurrency) -> MemoryRange.of(42, 1337));
        return Stream.of(
            Arguments.of(fixedMemory.build(), false),
            Arguments.of(fixedMemory.build(), true),
            Arguments.of(memoryRange.build(), false),
            Arguments.of(memoryRange.build(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("input")
    void doesNotThrow(MemoryEstimation estimation, boolean useMaxMemoryUsage) {
        var memoryTree = estimation.estimate(TEST_DIMENSIONS, 1);
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertDoesNotThrow(() -> MemoryValidation.validateMemoryUsage(
            memoryTreeWithDimensions,
            10_000,
            useMaxMemoryUsage
        ));
    }

    @ParameterizedTest
    @MethodSource("input")
    void throwsOnMinUsageExceeded(MemoryEstimation estimation, boolean ignored) {
        var memoryTree = estimation.estimate(TEST_DIMENSIONS, 1);
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertThatThrownBy(() -> MemoryValidation.validateMemoryUsage(
            memoryTreeWithDimensions,
            1,
            false
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since minimum estimated memory");
    }

    @ParameterizedTest
    @MethodSource("input")
    void throwsOnMaxUsageExceeded(MemoryEstimation estimation, boolean ignored) {
        var memoryTree = estimation.estimate(TEST_DIMENSIONS, 1);
        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, TEST_DIMENSIONS);

        assertThatThrownBy(() -> MemoryValidation.validateMemoryUsage(
            memoryTreeWithDimensions,
            1,
            true
        ))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Procedure was blocked since maximum estimated memory")
            .hasMessageContaining(
                "Consider resizing your Aura instance via console.neo4j.io. " +
                "Alternatively, use 'sudo: true' to override the memory validation. " +
                "Overriding the validation is at your own risk. " +
                "The database can run out of memory and data can be lost."
            );
    }

}
