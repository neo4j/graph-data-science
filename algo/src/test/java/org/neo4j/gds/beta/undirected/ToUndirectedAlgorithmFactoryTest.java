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
package org.neo4j.gds.beta.undirected;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ToUndirectedAlgorithmFactoryTest {

    static Stream<Arguments> compressions() {
        TestMethodRunner compressedRunner = TestMethodRunner::runCompressedOrdered;
        TestMethodRunner uncompressedRunner = TestMethodRunner::runUncompressedOrdered;

        return Stream.of(
            Arguments.of(compressedRunner, MemoryRange.of(1_724_488)),
            Arguments.of(uncompressedRunner, MemoryRange.of(3_035_304))
        );
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void memoryEstimationWithUncompressedFeatureToggle(TestMethodRunner runner, MemoryRange expected) {
        var factory = new ToUndirectedAlgorithmFactory();
        var config = ToUndirectedConfigImpl.builder().relationshipType("T1").mutateRelationshipType("R").build();

        GraphDimensions graphDimensions = GraphDimensions.of(100_000, 100_000);

        runner.run(() -> {
            var memoryEstimation = factory.memoryEstimation(config);

            var actual = memoryEstimation.estimate(graphDimensions, config.concurrency());
            assertThat(actual.memoryUsage().min).isEqualTo(expected.min);
            assertThat(actual.memoryUsage().max).isEqualTo(expected.max);
        });
    }

}
