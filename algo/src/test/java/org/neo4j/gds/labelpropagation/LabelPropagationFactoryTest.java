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
package org.neo4j.gds.labelpropagation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;

class LabelPropagationFactoryTest {
    private static final LabelPropagationStreamConfig DEFAULT_CONFIG = LabelPropagationStreamConfig.of(CypherMapWrapper.empty());

    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 800_464, 4_994_640),
            Arguments.of(4, 801_544, 17_578_248),
            Arguments.of(42, 815_224, 176_970_616)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        assertMemoryEstimation(
            () -> new LabelPropagationFactory<>().memoryEstimation(DEFAULT_CONFIG),
            100_000L,
            concurrency,
            MemoryRange.of(expectedMinBytes, expectedMaxBytes)
        );
    }

    @Test
    void shouldBoundMemEstimationToMaxSupportedDegree() {
        var labelPropagationFactory = new LabelPropagationFactory<>();
        GraphDimensions largeDimensions = ImmutableGraphDimensions.builder()
            .nodeCount((long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE)
            .build();

        // test for no failure and no overflow
        assertTrue(0 < labelPropagationFactory
            .memoryEstimation(ImmutableLabelPropagationStreamConfig.builder().build())
            .estimate(largeDimensions, 1)
            .memoryUsage().max);
    }
}
