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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class JaccardSimilarityTest {

    @ParameterizedTest(name = "{2}")
    @MethodSource("listCollectors")
    void shouldPassAtAllCasesOfListInput(
        Collector<Number, ?, List<Number>> firstListCollector,
        Collector<Number, ?, List<Number>> secondListCollector,
        String label
    ) {
        var arr1 = new int[]{1,2,3};
        var arr2 = new int[]{1,2,3};
        List<Number> l1 = Arrays.stream(arr1).boxed().collect(firstListCollector);
        List<Number> l2 = Arrays.stream(arr2).boxed().collect(secondListCollector);

        var similarities = new SimilaritiesFunc();
        var jaccarded = similarities.jaccardSimilarity(l1, l2);
        assertThat(jaccarded).isEqualTo(1);
    }

    private static Stream<Arguments> listCollectors() {
        return Stream.of(
            Arguments.of(
                Collectors.toUnmodifiableList(), Collectors.toUnmodifiableList(), "Unmodifiable, Unmodifiable"
            ),
            Arguments.of(
                Collectors.toUnmodifiableList(), Collectors.toList(), "Unmodifiable, Modifiable"
            ),
            Arguments.of(
                Collectors.toList(), Collectors.toList(), "Modifiable, Modifiable"
            ),
            Arguments.of(
                Collectors.toList(), Collectors.toUnmodifiableList(), "Modifiable, Unmodifiable"
            )
        );
    }

}
