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
package org.neo4j.gds.junit;

import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedParameterResolverTest {

    @ExtendWith(MappedParameterResolver.class)
    static class PrimitiveParametersTest {

        private int captured;

        @BeforeEach
        void setup(int i) {
            this.captured = i;
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3})
        void test(int i) {
            assertThat(captured).isEqualTo(i);
        }
    }

    @ExtendWith(MappedParameterResolver.class)
    static class EnumParameterTest {

        private TestEnum captured;

        @BeforeEach
        void setup(TestEnum testEnum) {
            this.captured = testEnum;
        }

        @ParameterizedTest
        @EnumSource(TestEnum.class)
        void test(TestEnum testEnum) {
            assertThat(captured).isEqualTo(testEnum);
        }

        enum TestEnum {
            A, B, C
        }
    }

    @ExtendWith(MappedParameterResolver.class)
    static class StreamSourceTest {

        private int capturedInt;
        private String capturedString;

        @BeforeEach
        void setup(int i, String j) {
            this.capturedInt = i;
            this.capturedString = j;
        }

        @ParameterizedTest
        @MethodSource("argumentsStream")
        void test(int i, String j) {
            assertThat(capturedInt).isEqualTo(i);
            assertThat(capturedString).isEqualTo(j);
        }

        static Stream<Arguments> argumentsStream() {
            return Stream.of(
                Arguments.of(1, "one"),
                Arguments.of(2, "two"),
                Arguments.of(3, "three")
            );
        }
    }

    @ExtendWith(MappedParameterResolver.class)
    static class AfterEachTest {
        private IntFunction<Integer> captured;

        @AfterEach
        void teardown(int i) {
            assertThat(captured.intValueOf(i)).isEqualTo(2 * i);
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3})
        void test(int i) {
            this.captured = integer -> 2 * i;
        }
    }
}
