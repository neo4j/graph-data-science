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
package org.neo4j.gds.similarity.knn.metrics;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class SimilarityMetricTest {

    @ParameterizedTest
    @MethodSource("metrics")
    void parse(String userInput, SimilarityMetric expectedMetric) {
        assertThat(SimilarityMetric.parse(userInput)).isEqualTo(expectedMetric);
    }

    public static Stream<Arguments> metrics() {
        return Stream.of(
            Arguments.of("JACCARD", SimilarityMetric.JACCARD),
            Arguments.of("Jaccard", SimilarityMetric.JACCARD),
            Arguments.of("overlap", SimilarityMetric.OVERLAP),
            Arguments.of("cosinE", SimilarityMetric.COSINE),
            Arguments.of("Euclidean", SimilarityMetric.EUCLIDEAN),
            Arguments.of("pearson", SimilarityMetric.PEARSON)
        );
    }
}
