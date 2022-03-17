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
package org.neo4j.gds.similarity;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SimilaritiesFuncTest {

    @Test
    void testCosineSimilarityOppositeDirections() {
        var left = new ArrayList<Number>(Arrays.asList(1, 1));
        var right = new ArrayList<Number>(Arrays.asList(-1, -1));
        double actual = new SimilaritiesFunc().cosineSimilarity(left, right);
        assertEquals(-1.0, actual);

    }

    @ParameterizedTest
    @MethodSource("listsWithDuplicates")
    void jaccardSimilarityShouldWorkWithDuplicates(List<Number> left, List<Number> right, double expected) {
        double actual = new SimilaritiesFunc().jaccardSimilarity(left, right);
        assertEquals(expected, actual);
    }

    static Stream<Arguments> listsWithDuplicates() {
        return Stream.of(
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 1)),
                new ArrayList<Number>(Arrays.asList(1, 2)),
                1 / 3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 1, 2)),
                new ArrayList<Number>(Arrays.asList(1, 3, 3)),
                1 / 5D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2)),
                new ArrayList<Number>(Arrays.asList(2, 1)),
                2 / 2D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE, 1)),
                new ArrayList<Number>(Arrays.asList((double) Long.MAX_VALUE - 1, 1)),
                1 / 3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE, 1)),
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE - 1, 1)),
                1 / 3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE, 1)),
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE - 1, 1)),
                1 / 3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(16605, 16605, 16605, 150672)),
                new ArrayList<Number>(Arrays.asList(16605, 16605, 150672, 16605)),
                4 / 4D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159.0, 4159, 4159.0, 4159)),
                new ArrayList<Number>(Arrays.asList(4159, 4159.0, 4159, 1337.0)),
                3 / 5D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159, 1337, 1337, 1337)),
                new ArrayList<Number>(Arrays.asList(1337, 4159, 4159, 4159)),
                2 / 6D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2, 2)),
                new ArrayList<Number>(Arrays.asList(2, 2, 3)),
                2 / 4D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(null, 2, 2)),
                new ArrayList<Number>(Arrays.asList(2, 2, null, null)),
                1D
            ), Arguments.of(
                new ArrayList<Number>(),
                new ArrayList<Number>(),
                1D
            )
        );
    }

    @Test
    void testCosineWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertThat(new SimilaritiesFunc().cosineSimilarity(left, right))
            .isCloseTo(0.8485, Offset.offset(0.001));
    }

    @Test
    void testEuclideanSimilarityWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertThat(new SimilaritiesFunc().euclideanSimilarity(left, right))
            .isCloseTo(0.3660, Offset.offset(0.001));
    }

    @Test
    void testEuclideanDistanceWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertThat(new SimilaritiesFunc().euclideanDistance(left, right))
            .isCloseTo(1.732, Offset.offset(0.001));
    }

    @Test
    void testPearsonWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertThat(new SimilaritiesFunc().pearsonSimilarity(left, right))
            .isCloseTo(0.6546, Offset.offset(0.001));
    }

    @Test
    void testOverlapWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertEquals(0.5, new SimilaritiesFunc().overlapSimilarity(left, right));
    }

    @Test
    void testJaccardWithNulls() {
        var left = new ArrayList<Number>(Arrays.asList(null, 1, 3));
        var right = new ArrayList<Number>(Arrays.asList(1, null, 2));
        assertEquals(1 / 3.0, new SimilaritiesFunc().jaccardSimilarity(left, right));
    }

}
