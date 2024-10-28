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
package org.neo4j.gds.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.schema.Direction;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GraphCharacteristicsTest {

    @Test
    void emptyCharacteristics() {
        var characteristics = GraphCharacteristics.builder().build();
        assertThat(characteristics.isUndirected()).isFalse();
        assertThat(characteristics.isDirected()).isFalse();
        assertThat(characteristics.isInverseIndexed()).isFalse();

        assertThat(characteristics).isEqualTo(GraphCharacteristics.NONE);
    }

    @Test
    void nonEmptyCharacteristics() {
        var characteristics = GraphCharacteristics.builder().directed().build();
        assertThat(characteristics.isDirected()).isTrue();
        assertThat(characteristics.isUndirected()).isFalse();
        assertThat(characteristics.isInverseIndexed()).isFalse();

        characteristics = GraphCharacteristics.builder().directed().undirected().build();
        assertThat(characteristics.isDirected()).isTrue();
        assertThat(characteristics.isUndirected()).isTrue();
        assertThat(characteristics.isInverseIndexed()).isFalse();

        characteristics = GraphCharacteristics.builder().directed().undirected().inverseIndexed().build();
        assertThat(characteristics.isDirected()).isTrue();
        assertThat(characteristics.isUndirected()).isTrue();
        assertThat(characteristics.isInverseIndexed()).isTrue();
    }

    @Test
    void intersect() {
        var lhs = GraphCharacteristics.builder().directed().inverseIndexed().build();
        var rhs = GraphCharacteristics.builder().directed().build();

        assertThat(lhs.intersect(rhs)).satisfies(characteristics -> {
            assertThat(characteristics.isDirected()).isTrue();
            assertThat(characteristics.isInverseIndexed()).isFalse();
            assertThat(characteristics.isUndirected()).isFalse();
        });
    }

    static Stream<Arguments> orientation() {
        return Stream.of(
            Arguments.of(Orientation.NATURAL, GraphCharacteristics.builder().directed().build()),
            Arguments.of(Orientation.REVERSE, GraphCharacteristics.builder().directed().build()),
            Arguments.of(Orientation.UNDIRECTED, GraphCharacteristics.builder().undirected().build())
        );
    }

    @ParameterizedTest
    @MethodSource("orientation")
    void buildFromOrientation(Orientation orientation, GraphCharacteristics expected) {
        var actual = GraphCharacteristics.builder().withOrientation(orientation).build();
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> direction() {
        return Stream.of(
            Arguments.of(Direction.DIRECTED, GraphCharacteristics.builder().directed().build()),
            Arguments.of(Direction.UNDIRECTED, GraphCharacteristics.builder().undirected().build())
        );
    }

    @ParameterizedTest
    @MethodSource("direction")
    void buildFromDirection(Direction direction, GraphCharacteristics expected) {
        var actual = GraphCharacteristics.builder().withDirection(direction).build();
        assertThat(actual).isEqualTo(expected);
    }
}
