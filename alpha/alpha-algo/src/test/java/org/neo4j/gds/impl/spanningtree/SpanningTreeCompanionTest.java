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
package org.neo4j.gds.impl.spanningtree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SpanningTreeCompanionTest {

    static Stream<Arguments> operators() {
        return Stream.of(
            arguments("maximum", Prim.MAX_OPERATOR),
            arguments("minimum", Prim.MIN_OPERATOR)
        );
    }

    @ParameterizedTest
    @MethodSource("operators")
    void conversionsWorkWell(String stringOperator, DoubleUnaryOperator doubleUnaryOperator) {
        assertThat(SpanningTreeCompanion.parse(stringOperator)).isEqualTo(doubleUnaryOperator);
        assertThat(SpanningTreeCompanion.toString(doubleUnaryOperator)).isEqualTo(stringOperator);
    }

    @Test
    void shouldThrowForIncorrectInput() {
        assertThatThrownBy(() -> SpanningTreeCompanion.parse("foo")).hasMessageContaining(
            "Input value `foo` for parameter `objective` is not supported. Must be one of: [maximum, minimum]");
    }

}
