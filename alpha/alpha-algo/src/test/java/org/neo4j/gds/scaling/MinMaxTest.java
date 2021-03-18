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
import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.core.concurrency.Pools;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MinMaxTest {

    private static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of((DoubleNodeProperties) nodeId -> nodeId, 0D, 9D),
            Arguments.of((LongNodeProperties) nodeId -> nodeId, 0D, 9D),
            Arguments.of((DoubleNodeProperties) nodeId -> 50000000D * nodeId, 0D, 4.5e8)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodeProperties properties, double min, double max) {
        var minMaxNormalizer = (MinMax) MinMax.create(properties, 10, 1, Pools.DEFAULT);

        assertThat(minMaxNormalizer.min).isEqualTo(min);
        assertThat(minMaxNormalizer.maxMinDiff).isEqualTo(max - min);

        for (int i = 0; i < 10; i++) {
            assertThat(minMaxNormalizer.scaleProperty(i)).isEqualTo(i / 9D);
        }
    }

    @Test
    void avoidsDivByZero() {
        var properties = (DoubleNodeProperties) nodeId -> 4D;
        var minMaxNormalizer = MinMax.create(properties, 10, 1, Pools.DEFAULT);

        for (int i = 0; i < 10; i++) {
            assertThat(minMaxNormalizer.scaleProperty(i)).isEqualTo(0D);
        }
    }

}
