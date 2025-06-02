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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Node2VecBaseConfigTest {

    private static Stream<Arguments> invalidEmbeddingInitializer() {
        return Stream.of(
            Arguments.of(1, "Expected EmbeddingInitializer or String. Got Integer."),
            Arguments.of("alwaysTrue", "EmbeddingInitializer `alwaysTrue` is not supported. Must be one of: ['NORMALIZED', 'UNIFORM'].")
        );
    }

    @ParameterizedTest
    @MethodSource("invalidEmbeddingInitializer")
    void failOninvalidEmbeddingInitializer(Object embeddingInitializer, String errorMessage) {
        var mapWrapper = CypherMapWrapper.create(Map.of("modelName", "foo", "embeddingInitializer", embeddingInitializer));
        assertThatThrownBy(() -> Node2VecMutateConfig.of(mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(errorMessage);
    }

}
