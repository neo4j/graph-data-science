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
package org.neo4j.gds.similarity.nodesim.metrics;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMetric;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityParameters;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetricSimilarityComputerFactoryTest {

    static Stream<Arguments> metrics() {
        return Stream.of(
            arguments( NodeSimilarityMetric.COSINE,  CosineSimilarityComputer.class),
            arguments(NodeSimilarityMetric.JACCARD, JaccardSimilarityComputer.class),
            arguments(NodeSimilarityMetric.OVERLAP, OverlapSimilarityComputer.class)
        );
    }

    @ParameterizedTest
    @MethodSource("metrics")
    void shouldCreateCorrectly(NodeSimilarityMetric metric,Class<?> expected) {

        var params = mock(NodeSimilarityParameters.class);
        when(params.metric()).thenReturn(metric);
        when(params.similarityCutoff()).thenReturn(0d);
        var computer = MetricSimilarityComputerFactory.create(
            params
        );

        assertThat(computer).isInstanceOf(expected);
    }

}
