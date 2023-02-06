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
package org.neo4j.gds.paths.yens;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.paths.ImmutablePathResult;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MutablePathResultTest {

    private static final MutablePathResult TEST_PATH = MutablePathResult.of(ImmutablePathResult
        .builder()
        .nodeIds(0, 4, 2, 3)
        .relationshipIds(0, 1, 2)
        .costs(1, 3, 4, 7)
        .sourceNode(0)
        .targetNode(3)
        .index(0)
        .build()
    );

    static MutablePathResult testPath(long... nodeIds) {
        return MutablePathResult.of(ImmutablePathResult
            .builder()
            .nodeIds(nodeIds)
            .costs(IntStream.range(0, nodeIds.length).asDoubleStream().toArray())
            .sourceNode(nodeIds[0])
            .targetNode(nodeIds[nodeIds.length - 1])
            .relationshipIds(Arrays.copyOf(nodeIds, nodeIds.length - 1))
            .index(0)
            .build()
        );
    }

    @Test
    void nodeCount() {
        assertEquals(4, TEST_PATH.nodeCount());
    }

    @Test
    void nodeAtIndex() {
        assertEquals(0, TEST_PATH.node(0));
        assertEquals(4, TEST_PATH.node(1));
        assertEquals(2, TEST_PATH.node(2));
        assertEquals(3, TEST_PATH.node(3));
    }

    @Test
    void nodeAtIndexOutOfBounds() {
        assertThatThrownBy(() -> TEST_PATH.node(-1)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> TEST_PATH.node(4)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void equals() {
        assertEquals(testPath(0), testPath(0));
        assertEquals(testPath(4, 2), testPath(4, 2));
        assertEquals(testPath(1, 3, 3, 7), testPath(1, 3, 3, 7));
    }

    @Test
    void notEquals() {
        assertNotEquals(testPath(0), testPath(1));
        assertNotEquals(testPath(4, 2), testPath(2));
    }

    @Test
    void sameHashCode() {
        assertEquals(testPath(0).hashCode(), testPath(0).hashCode());
        assertEquals(testPath(4, 2).hashCode(), testPath(4, 2).hashCode());
        assertEquals(testPath(1, 3, 3, 7).hashCode(), testPath(1, 3, 3, 7).hashCode());
    }

    @Test
    void differentHashCode() {
        assertNotEquals(testPath(0).hashCode(), testPath(1).hashCode());
        assertNotEquals(testPath(4, 2).hashCode(), testPath(2).hashCode());
    }

    @Test
    void totalCost() {
        assertEquals(7.0D, TEST_PATH.totalCost());
    }

    static Stream<Arguments> matchesInput() {
        return Stream.of(
            Arguments.of(testPath(0, 1, 3), testPath(0, 1, 4), 1, true),
            Arguments.of(testPath(0, 1, 3), testPath(0, 1, 4), 2, true),
            Arguments.of(testPath(0, 1, 3), testPath(0, 1, 4), 3, false),
            Arguments.of(testPath(0), testPath(1), 1, false),
            Arguments.of(testPath(0), testPath(0), 0, true)
        );
    }

    @ParameterizedTest
    @MethodSource("matchesInput")
    void matches(MutablePathResult first, MutablePathResult second, int index, boolean expected) {
        assertEquals(expected, first.matches(second, index));
    }

    @Test
    void matchesOutOfBounds() {
        var path = testPath(0, 1, 3);
        assertThatThrownBy(() -> path.matches(path, 5)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    @Test
    void withIndex() {
        var testPath = MutablePathResult.of(ImmutablePathResult
            .builder()
            .nodeIds(0, 1, 2, 3)
            .relationshipIds(0, 1, 2)
            .costs(1, 3, 4, 7)
            .sourceNode(0)
            .targetNode(3)
            .index(0)
            .build()
        );
        assertEquals(0, testPath.index());
        testPath.withIndex(42);
        assertEquals(42, testPath.index());
        testPath.withIndex(0);
        assertEquals(0, testPath.index());
    }

    static Stream<Arguments> subPathInput() {
        return Stream.of(
            Arguments.of(testPath(0, 1, 2, 3), 1, testPath(0)),
            Arguments.of(testPath(0, 1, 2, 3), 2, testPath(0, 1)),
            Arguments.of(testPath(0, 1, 2, 3), 3, testPath(0, 1, 2)),
            Arguments.of(testPath(0, 1, 2, 3), 4, testPath(0, 1, 2, 3))
        );
    }

    @ParameterizedTest
    @MethodSource("subPathInput")
    void subPath(MutablePathResult input, int index, MutablePathResult expected) {
        assertEquals(expected, input.subPath(index));
    }

    @Test
    void append() {
        var path1 = testPath(0, 1, 2);
        var path2 = testPath(2, 3, 4);

        path1.append(path2, true);
        var expected = testPath(0, 1, 2, 3, 4);

        assertEquals(expected, path1);
    }

    @Test
    void appendWithOffset() {
        //@formatter:off
        var p1 = MutablePathResult.of(ImmutablePathResult.builder().nodeIds(0, 1, 2).relationshipIds(0, 1).costs(0, 1, 42).sourceNode(0).targetNode(2).index(0).build());
        var p2 = MutablePathResult.of(ImmutablePathResult.builder().nodeIds(2, 3, 4).relationshipIds(2, 3).costs(0, 13, 37).sourceNode(0).targetNode(2).index(1).build());
        var expected = MutablePathResult.of(ImmutablePathResult.builder().nodeIds(0, 1, 2, 3, 4).relationshipIds(0, 1, 2, 3).costs(0, 1, 42, 55, 79).sourceNode(0).targetNode(2).index(2).build());
        //@formatter:on

        p1.append(p2, true);

        assertEquals(expected, p1);
        assertEquals(expected.totalCost(), p1.totalCost());
    }

    @Test
    void appendFailedAssertion() {
        var path1 = testPath(0, 1, 2);
        var path2 = testPath(4, 3, 5);

        assertThatThrownBy(() -> path1.append(path2, true)).isInstanceOf(AssertionError.class);
    }
}
