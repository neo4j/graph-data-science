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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.concurrency.Pools;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class NoneScalerTest {

    private static Stream<Arguments> properties() {
        double[] expected = {4, 6, 8, 10};
        return Stream.of(
            Arguments.of((DoubleNodeProperties) nodeId -> 2 * (nodeId + 1), expected)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void scale(NodeProperties properties, double[] expected) {
        var scaler = Scaler.Variant.NONE.create(properties, 4, 42, Pools.DEFAULT);

        double[] actual = new double[5];
        IntStream.range(0, 5).forEach(nodeId -> scaler.scaleProperty(nodeId, actual, nodeId));
        assertThat(actual).containsSequence(expected);
    }

}
