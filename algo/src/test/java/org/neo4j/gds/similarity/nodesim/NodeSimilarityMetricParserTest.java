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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NodeSimilarityMetricParserTest {

    @Test
    void shouldThrowOnWrongMetric2() {
        assertThatThrownBy(() -> NodeSimilarityMetricParser.parse("ovErLaPPPPP"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ovErLaPPPPP is not a valid metric");
    }

    static Stream<Arguments> metrics() {
        return Stream.of(
            arguments("CosiNe",NodeSimilarityMetric.COSINE),
            arguments("jaccard",NodeSimilarityMetric.JACCARD),
            arguments("OVERLAP",NodeSimilarityMetric.OVERLAP)
        );
    }

    @ParameterizedTest
    @MethodSource("metrics")
    void shouldParseCorrectly(String input, NodeSimilarityMetric expected) {
        var metric = NodeSimilarityMetricParser.parse(input);
        assertThat(metric).isEqualTo(expected);
    }
}
