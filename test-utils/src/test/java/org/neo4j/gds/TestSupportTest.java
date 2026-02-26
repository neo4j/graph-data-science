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
package org.neo4j.gds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TestSupportTest {

    @Test
    void testBinaryCrossArguments() {
        Supplier<Stream<Arguments>> leftFn = () -> Stream.of(Arguments.of(1, 2), Arguments.of(3, 4));
        Supplier<Stream<Arguments>> rightFn = () -> Stream.of(Arguments.of("A", "B"), Arguments.of("C", "D"));
        Stream<Arguments> crossStream = TestSupport.crossArguments(leftFn, rightFn);

        Stream<Arguments> expectedStream = Stream.of(
            Arguments.of(1, 2, "A", "B"),
            Arguments.of(1, 2, "C", "D"),
            Arguments.of(3, 4, "A", "B"),
            Arguments.of(3, 4, "C", "D")
        );

        Assertions.assertEquals(collectArguments(expectedStream), collectArguments(crossStream));
    }

    @Test
    void testNaryCrossArguments() {
        Supplier<Stream<Arguments>> fn1 = () -> Stream.of(Arguments.of(1), Arguments.of(2));
        Supplier<Stream<Arguments>> fn2 = () -> Stream.of(Arguments.of("A"), Arguments.of("B"));
        Supplier<Stream<Arguments>> fn3 = () -> Stream.of(Arguments.of(true), Arguments.of(false));
        Stream<Arguments> crossStream = TestSupport.crossArguments(fn1, fn2, fn3);

        Stream<Arguments> expectedStream = Stream.of(
            Arguments.of(1, "A", true),
            Arguments.of(1, "A", false),
            Arguments.of(1, "B", true),
            Arguments.of(1, "B", false),
            Arguments.of(2, "A", true),
            Arguments.of(2, "A", false),
            Arguments.of(2, "B", true),
            Arguments.of(2, "B", false)
        );

        Assertions.assertEquals(collectArguments(expectedStream), collectArguments(crossStream));
    }

    @Test
    void fromGdlTest() {
        var g = TestSupport.fromGdl("(a)");
        assertThat(g.toOriginalNodeId("a")).isEqualTo(0L);
    }

    @Test
    void fromGdlWithIdOffsetTest() {
        var g = TestSupport.fromGdl("(a)", 42L);
        assertThat(g.toOriginalNodeId("a")).isEqualTo(42L);
    }

    private Set<List<Object>> collectArguments(Stream<Arguments> arguments) {
        return arguments.map(Arguments::get).map(Arrays::asList).collect(Collectors.toSet());
    }

    @Test
    void gdlFromGraphStoreTest() {
        String gdlInput = """
            (a0:foo:high {height : 1.97d})
            (a1:bar:baz {fibo : [0L, 1L, 1L, 2L, 3L, 5L, 8L]})
            (a2)
            (a3:heavy {weight : 66L})
            (a4)
            (a0)-->(a0)
            (a0)-[{greatness : 0.001}]->(a1)
            (a0)-[:foo]->(a0)
            (a0)-[:foo]->(a0)
            (a0)-[:likes {intensity : 0.3, validity: 0.1}]->(a1)
            (a1)-[:likes {intensity : 0.1, validity: 0.9}]->(a0)
            (a1)-[:dislikes]->(a0)
            """;

        var gs1 = GdlFactory.of(gdlInput).build();

        String gdlActual = TestSupport.gdlFromGraphStore(gs1);

        var gs2 = GdlFactory.of(gdlActual).build();
        TestSupport.assertGraphEquals(gs1.getUnion(), gs2.getUnion());
    }
}
