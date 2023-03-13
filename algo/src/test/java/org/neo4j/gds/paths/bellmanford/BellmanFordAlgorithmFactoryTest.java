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
package org.neo4j.gds.paths.bellmanford;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.TestSupport.assertMemoryEstimation;

class BellmanFordAlgorithmFactoryTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("memoryEstimationSetup")
    void memoryEstimation(String description, boolean mutateNegativeCycles, long expectedBytes) {
        var config = BellmanFordMutateConfig.of(CypherMapWrapper.create(
            Map.of(
                "sourceNode", 0L,
                "mutateNegativeCycles", mutateNegativeCycles,
                "mutateRelationshipType", "foo"
            )
        ));
        var algorithmFactory = new BellmanFordAlgorithmFactory<>();

        assertMemoryEstimation(
            () -> algorithmFactory.memoryEstimation(config),
            10,
            23,
            4,
            MemoryRange.of(expectedBytes)
        );
    }

    static Stream<Arguments> memoryEstimationSetup() {
        return Stream.of(
            Arguments.of("Track negative cycles", true, 1520),
            Arguments.of("Don't track negative cycles", false, 1400)
        );
    }


}
