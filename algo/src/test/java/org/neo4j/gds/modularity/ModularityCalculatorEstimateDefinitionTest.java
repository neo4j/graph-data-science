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
package org.neo4j.gds.modularity;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.assertions.MemoryEstimationAssert;

import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

class ModularityCalculatorEstimateDefinitionTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("memoryEstimationSetup")
    void memoryEstimation(int concurrency,long expected) {
        var config = mock(ModularityBaseConfig.class);

        var memoryEstimation = new ModularityCalculatorMemoryEstimateDefinition().memoryEstimation(config);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(10, 23, concurrency)
            .hasSameMinAndMaxEqualTo(expected);
    }

    static Stream<Arguments> memoryEstimationSetup() {
        return Stream.of(
            Arguments.of(1, 960),
            Arguments.of(4, 1_080)
        );
    }

}
