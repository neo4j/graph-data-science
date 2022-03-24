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
package org.neo4j.gds.similarity.knn;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KnnNodePropertySpecParserTest {

    @Test
    void shouldParseString() {
        var input = "property";
        assertThat(KnnNodePropertySpecParser.parse(input))
            .singleElement()
            .extracting(KnnNodePropertySpec::name)
            .isEqualTo("property");
    }

    @Test
    void shouldParseListOfSingleString() {
        var input = List.of("property");
        assertThat(KnnNodePropertySpecParser.parse(input))
            .singleElement()
            .extracting(KnnNodePropertySpec::name)
            .isEqualTo("property");
    }

    @Test
    void shouldParseListOfMultipleStrings() {
        var input = List.of("property1", "property2", "property3");
        assertThat(KnnNodePropertySpecParser.parse(input))
            .hasSize(3)
            .extracting(KnnNodePropertySpec::name)
            .containsExactlyInAnyOrder("property1", "property2", "property3");
    }

    @Test
    void shouldParseListOfMapsAndStrings() {
        var input = List.of(
            "property1",
            Map.of("property2", "JACCARD"),
            "property3",
            Map.of("property4", "PEARSON")
        );
        var specs = KnnNodePropertySpecParser.parse(input);
        assertThat(specs)
            .hasSize(4)
            .extracting(KnnNodePropertySpec::name)
            .containsExactlyInAnyOrder("property1", "property2", "property3", "property4");
        assertThat(specs)
            .extracting(KnnNodePropertySpec::metric)
            .containsExactlyInAnyOrder(
                SimilarityMetric.DEFAULT,
                SimilarityMetric.DEFAULT,
                SimilarityMetric.JACCARD,
                SimilarityMetric.PEARSON
            );
    }

    @Test
    void shouldParseMap() {
        var input = Map.of(
            "property1", "OVERLAP",
            "property2", "COSINE",
            "property3", "EUCLIDEAN",
            "property4", "DEFAULT"
        );
        var specs = KnnNodePropertySpecParser.parse(input);
        assertThat(specs)
            .hasSize(4)
            .extracting(KnnNodePropertySpec::name)
            .containsExactlyInAnyOrder("property1", "property2", "property3", "property4");
        assertThat(specs)
            .extracting(KnnNodePropertySpec::metric)
            .containsExactlyInAnyOrder(
                SimilarityMetric.COSINE,
                SimilarityMetric.DEFAULT,
                SimilarityMetric.EUCLIDEAN,
                SimilarityMetric.OVERLAP
            );
    }

    @ParameterizedTest
    @MethodSource("metrics")
    void shouldAcceptMetricsRegardlessOfCase(String metric, SimilarityMetric expectedMetricValue) {
        var input = "property";
        assertThat(KnnNodePropertySpecParser.parse(Map.of(input, metric)))
            .singleElement()
            .extracting(KnnNodePropertySpec::metric)
            .isEqualTo(expectedMetricValue);
    }

    public static Stream<Arguments> metrics() {
        return Stream.of(
            Arguments.of("cosine", SimilarityMetric.COSINE),
            Arguments.of("euCLIDean", SimilarityMetric.EUCLIDEAN),
            Arguments.of("jACcaRd", SimilarityMetric.JACCARD),
            Arguments.of("OVERLAP", SimilarityMetric.OVERLAP),
            Arguments.of("Pearson", SimilarityMetric.PEARSON)
        );
    }

    @Test
    void shouldRefuseToParseEmptyList() {
        var input = List.of();
        assertThatThrownBy(() -> KnnNodePropertySpecParser.parse(input))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not be empty");
    }

    @Test
    void shouldShouldRefuseToParseInvalidPropertyName() {
        var input = List.of(" ");
        assertThatThrownBy(() -> KnnNodePropertySpecParser.parse(input))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not end or begin with whitespace");
    }

    @Test
    void shouldRefuseToParseInvalidSimilarityMetric() {
        var input = Map.of("property", "INVALID");
        assertThatThrownBy(() -> KnnNodePropertySpecParser.parse(input))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No valid similarity metric for user input INVALID");
    }

    @Test
    void shouldRenderPropertySpecs() {
        var input = List.of("property1", Map.of("property2", "PEARSON"));
        var specs = KnnNodePropertySpecParser.parse(input);
        assertThat(KnnNodePropertySpecParser.render(specs)).isEqualTo(
            Map.of(
                "property1", "DEFAULT",
                "property2", "PEARSON"
            )
        );
    }
}
