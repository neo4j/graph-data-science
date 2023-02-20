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
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class L1NormTest {

    private static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of(
                5,
                new DoubleTestPropertyValues(nodeId -> nodeId),
                10D,
                new double[]{0, 0.1D, 0.2D, 0.3D, 0.4D}
            ),
            Arguments.of(
                5,
                new DoubleTestPropertyValues(nodeId ->(nodeId % 2 == 0) ? -nodeId : nodeId),
                10D,
                new double[]{0, 0.1D, - 0.2D, 0.3D, -0.40D}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void scale(int nodeCount, NodePropertyValues properties, double l1norm, double[] expected) {
        var scaler = (L1Norm) L1Norm.buildFrom(CypherMapWrapper.empty()).create(properties, nodeCount, 1, Pools.DEFAULT);

        assertThat(scaler.l1Norm).isEqualTo(l1norm);

        double[] actual = IntStream.range(0, nodeCount).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void avoidsDivByZero() {
        var properties = new DoubleTestPropertyValues(nodeId -> 0D);
        var scaler = L1Norm.buildFrom(CypherMapWrapper.empty()).create(properties, 10, 1, Pools.DEFAULT);

        for (int i = 0; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isEqualTo(0D);
        }
    }

}
