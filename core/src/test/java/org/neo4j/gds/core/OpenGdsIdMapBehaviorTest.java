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
package org.neo4j.gds.core;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;
import org.neo4j.gds.core.loading.GrowingArrayIdMapBuilder;

import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class OpenGdsIdMapBehaviorTest {

    static Stream<Arguments> idMapBuildersById() {
        return Stream.of(
            Arguments.of(ArrayIdMapBuilder.ID, Optional.empty(), Optional.empty(), GrowingArrayIdMapBuilder.class),
            Arguments.of(ArrayIdMapBuilder.ID, Optional.empty(), Optional.of(42L), ArrayIdMapBuilder.class),
            Arguments.of((byte) 42, Optional.empty(), Optional.empty(), GrowingArrayIdMapBuilder.class),
            Arguments.of((byte) 42, Optional.empty(), Optional.of(42L), ArrayIdMapBuilder.class)
        );
    }

    @ParameterizedTest
    @MethodSource("idMapBuildersById")
    void shouldCreateIdMapBuilderById(
        byte id,
        Optional<Long> maxOriginalId,
        Optional<Long> nodeCount,
        Class<?> idMapBuilderClazz
    ) {
        var idMapBuilder = new OpenGdsIdMapBehavior().create(id, 1, maxOriginalId, nodeCount);
        assertThat(idMapBuilder).isInstanceOf(idMapBuilderClazz);
    }

}
