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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
/**
 *
 * cosine similarity taken from here: https://neo4j.com/graphgist/a7c915c8-a3d6-43b9-8127-1836fecc6e2f
 * euclid distance taken from here: https://neo4j.com/blog/real-time-recommendation-engine-data-science/
 * euclid similarity taken from here: http://stats.stackexchange.com/a/158285
 * pearson similarity taken from here: http://guides.neo4j.com/sandbox/recommendations
 */
class SimilaritiesFuncTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerFunctions(SimilaritiesFunc.class);
    }

    @Test
    void testCosineSimilarityOppositeDirections() {
        runQueryWithResultConsumer(
            "RETURN gds.alpha.similarity.cosine([1.0, 1.0], [-1.0, -1.0]) AS similarity", result -> {
                assertThat(result.next().get("similarity")).isEqualTo(-1.0);
            });
    }



    @Test
    void testJaccardWithDuplicatesViaCypher() {
        runQueryWithResultConsumer(
            "RETURN gds.alpha.similarity.jaccard([1, 1, 2], [1, 3, 3]) AS jaccardSim", result -> {
                assertEquals(1/5D, result.next().get("jaccardSim"));
            });
    }

    @ParameterizedTest
    @MethodSource("listsWithDuplicates")
    void jaccardSimilarityShouldUseListsNotSets(List<Number> left, List<Number> right, double expected) {
        double actual = new SimilaritiesFunc().jaccardSimilarity(left, right);
        assertEquals(expected, actual);
    }

    static Stream<Arguments> listsWithDuplicates() {
        return Stream.of(
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 1)),
                new ArrayList<Number>(Arrays.asList(1, 2)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2)),
                new ArrayList<Number>(Arrays.asList(2, 1)),
                2/2D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE,              1)),
                new ArrayList<Number>(Arrays.asList((double) Long.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE,     1)),
                new ArrayList<Number>(Arrays.asList(Long.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE,     1)),
                new ArrayList<Number>(Arrays.asList(Integer.MAX_VALUE - 1, 1)),
                1/3D
            ),
            Arguments.of(
                new ArrayList<Number>(Arrays.asList(16605, 16605, 16605, 150672)),
                new ArrayList<Number>(Arrays.asList(16605, 16605, 150672, 16605)),
                4/4D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159.0, 4159,   4159.0, 4159)),
                new ArrayList<Number>(Arrays.asList(4159,   4159.0, 4159,   1337.0)),
                3/5D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(4159, 1337, 1337, 1337)),
                new ArrayList<Number>(Arrays.asList(1337, 4159, 4159, 4159)),
                2/6D
            ), Arguments.of(
                new ArrayList<Number>(Arrays.asList(1, 2, 2)),
                new ArrayList<Number>(Arrays.asList(2, 2, 3)),
                2/4D
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
    void testSimilarityFunctionsHandlesNullValues() {
        runQuery("RETURN gds.alpha.similarity.cosine([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.euclidean([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.euclideanDistance([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.pearson([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.overlap([1, null, 2], [null, 1, 3])");

        runQuery("RETURN gds.alpha.similarity.jaccard([1, null, 2], [null, 1, 3])");
    }

}


