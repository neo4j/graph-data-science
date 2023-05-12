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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;

class FastRPStreamConfigTest {
    @Test
    void acceptsIntegerIterationWeights() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "embeddingDimension", 64,
                "iterationWeights", List.of(1, 2L, 3.0)
            )
        );

        assertThatNoException().isThrownBy(() -> FastRPStreamConfig.of(userInput));
    }


    @ParameterizedTest
    @MethodSource("invalidWeights")
    void validatesWeights(List<?> iterationWeights, String messagePart) {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "embeddingDimension", 64,
                "iterationWeights", iterationWeights
            )
        );

        assertThatIllegalArgumentException().isThrownBy(() -> FastRPStreamConfig.of(userInput))
            .withMessageContaining(messagePart);
    }

    private static Stream<Arguments> invalidWeights() {
        return Stream.of(
            Arguments.of(List.of(), "must not be empty"),
            Arguments.of(List.of(1, "2"), "Iteration weights must be numbers"),
            Arguments.of(Arrays.asList(1, null), "Iteration weights must be numbers")
        );
    }
}
