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
package org.neo4j.gds.kcore;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.GraphDimensions;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class KCoreDecompositionAlgorithmFactoryTest {

    static Stream<Arguments> memoryEstimationTuples() {
        return Stream.of(
            arguments(1, 1944L),
            arguments(4, 4896L)
        );
    }

    @ParameterizedTest
    @MethodSource("memoryEstimationTuples")
    void memoryEstimation(int concurrency, long expectedMemoryEstimation) {
        var config = KCoreDecompositionStreamConfigImpl.builder().concurrency(concurrency).build();
        var factory = new KCoreDecompositionAlgorithmFactory<>();
        var estimate = factory.memoryEstimation(config)
            .estimate(GraphDimensions.of(100), config.concurrency());

        var memoryUsage = estimate.memoryUsage();
        assertThat(memoryUsage.min).isEqualTo(expectedMemoryEstimation);
        assertThat(memoryUsage.max).isEqualTo(expectedMemoryEstimation);//4624L
    }

}
