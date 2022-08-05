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
package org.neo4j.gds.influenceMaximization;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.TestSupport.assertMemoryEstimation;

class CELFAlgorithmFactoryTest {

    @ParameterizedTest
    @MethodSource("configurations")
    void memoryEstimation(int seedSetSize, int concurrency, long expectedMemory) {
        var memoryEstimation = new CELFAlgorithmFactory<InfluenceMaximizationStreamConfig>().memoryEstimation(
            InfluenceMaximizationStreamConfig.of(
                CypherMapWrapper.create(
                    Map.of("seedSetSize", seedSetSize, "concurrency", concurrency)
                )));

        assertMemoryEstimation(
            () -> memoryEstimation,
            42,
            1337,
            concurrency,
            MemoryRange.of(expectedMemory, expectedMemory)
        );
    }

    static Stream<Arguments> configurations() {
        return Stream.of(
            Arguments.of(1, 1, 2_928),
            Arguments.of(10, 1, 3_176),
            Arguments.of(1, 2, 3_456),
            Arguments.of(10, 2, 3_704),
            Arguments.of(1, 4, 4_512),
            Arguments.of(10, 4, 4_760)

        );
    }
}
