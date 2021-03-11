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
package org.neo4j.gds.ml.normalizing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MeanNormalizerTest {

    private static Stream<Arguments> properties() {
        double[] expected = {-0.5, -7 / 18D, -5 / 18D, -3 / 18D, -1 / 18D, 1 / 18D, 3 / 18D, 5 / 18D, 7 / 18D, 0.5};
        return Stream.of(
            Arguments.of((DoubleNodeProperties) nodeId -> nodeId, 4.5D, 9D, expected),
            Arguments.of((LongNodeProperties) nodeId -> nodeId, 4.5D, 9D, expected),
            Arguments.of((DoubleNodeProperties) nodeId -> 50000000D * nodeId, 50000000D * 4.5D, 4.5e8, expected)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodeProperties properties, double avg, double maxMinDiff, double[] expected) {
        var normalizer = MeanNormalizer.create(properties, 10);

        assertThat(normalizer.avg).isEqualTo(avg);
        assertThat(normalizer.maxMinDiff).isEqualTo(maxMinDiff);

        double[] actual = IntStream.range(0, 10).mapToDouble(normalizer::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void avoidsDivByZero() {
        var properties = (DoubleNodeProperties) nodeId -> 4D;
        var normalizer = MeanNormalizer.create(properties, 10);

        assertThat(normalizer.avg).isEqualTo(4D);
        assertThat(normalizer.maxMinDiff).isEqualTo(0D);

        for (int i = 0; i < 10; i++) {
            assertThat(normalizer.scaleProperty(i)).isEqualTo(0D);
        }
    }

}
