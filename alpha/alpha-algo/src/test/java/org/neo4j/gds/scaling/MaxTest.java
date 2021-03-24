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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.concurrency.Pools;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MaxTest {

    private static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of(
                10L,
                (DoubleNodeProperties) nodeId -> nodeId,
                9D,
                new double[]{0, 1 / 9D, 2 / 9D, 3 / 9D, 4 / 9D, 5 / 9D, 6 / 9D, 7 / 9D, 8 / 9D, 9 / 9D}
            ),
            Arguments.of(
                10L,
                (DoubleNodeProperties) nodeId -> -nodeId,
                9D,
                new double[]{0, -1 / 9D, -2 / 9D, -3 / 9D, -4 / 9D, -5 / 9D, -6 / 9D, -7 / 9D, -8 / 9D, -9 / 9D}
            ),
            Arguments.of(
                5L,
                (DoubleNodeProperties) nodeId -> (nodeId % 2 == 0) ? -nodeId : nodeId,
                4D,
                new double[]{0, 1 / 4D, -0.5D, 3 / 4D, -1}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void scale(long nodeCount, NodeProperties properties, double maxAbs, double[] expected) {
        var scaler = (Max) Max.create(properties, nodeCount, 1, Pools.DEFAULT);

        assertThat(scaler.maxAbs).isEqualTo(maxAbs);

        double[] actual = LongStream.range(0, nodeCount).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void avoidsDivByZero() {
        var properties = (DoubleNodeProperties) nodeId -> 0D;
        var scaler = Max.create(properties, 10, 1, Pools.DEFAULT);

        for (int i = 0; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isEqualTo(0D);
        }
    }
}
