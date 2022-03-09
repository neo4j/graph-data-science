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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.knn.metrics.SimilarityMetric;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KnnBaseConfigTest {

    @Test
    void nodePropertiesShouldAcceptString() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", "property"
            )
        );
        assertThat(new KnnBaseConfigImpl(userInput).nodeProperties())
            .singleElement()
            .extracting(KnnNodePropertySpec::name)
            .isEqualTo("property");
    }

    @Test
    void nodePropertiesShouldAcceptList() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", List.of("property1", "property2")
            )
        );
        assertThat(new KnnBaseConfigImpl(userInput).nodeProperties())
            .extracting(KnnNodePropertySpec::name)
            .containsExactlyInAnyOrder("property1", "property2");
    }

    @Test
    void nodePropertiesShouldAcceptMap() {
        var userInput = CypherMapWrapper.create(
            Map.of(
                "nodeProperties", Map.of(
                    "property1", "JACCARD",
                    "property2", "PEARSON"
                )
            )
        );
        assertThat(new KnnBaseConfigImpl(userInput).nodeProperties())
            .hasSize(2)
            .satisfies((list) -> {
                var spec1 = list.get(0);
                var spec2 = list.get(1);
                assertThat(List.of(spec1.name(), spec2.name()))
                    .containsExactlyInAnyOrder("property1", "property2");
                assertThat(List.of(spec1.metric().get(), spec2.metric().get()))
                    .containsExactlyInAnyOrder(SimilarityMetric.JACCARD, SimilarityMetric.PEARSON);
            });
    }

    @Test
    void shouldThrowIfUserProvidesEmptyNodeProperties() {
        var map = CypherMapWrapper.create(Map.of(
            "nodeProperties", List.of()
        ));

        assertThatThrownBy(() -> new KnnBaseConfigImpl(map))
            .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void shouldThrowIfUserProvidesMalformedNodeProperties() {
        var map = CypherMapWrapper.create(Map.of(
            "nodeProperties", List.of(" ")
        ));

        assertThatThrownBy(() -> new KnnBaseConfigImpl(map))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("must not end or begin with whitespace");
    }

    @Test
    void shouldThrowIfUserProvidesInvalidSimilarityMetric() {
        var map = CypherMapWrapper.create(Map.of(
            "nodeProperties", Map.of("property", "INVALID")
        ));
        assertThatThrownBy(() -> new KnnBaseConfigImpl(map))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No valid similarity metric for user input INVALID");
    }
}
