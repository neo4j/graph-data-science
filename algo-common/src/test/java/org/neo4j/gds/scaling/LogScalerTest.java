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
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LogScalerTest {

    private static Stream<Arguments> properties() {
        double[] expected = {1, 2, 3, 4};
        return Stream.of(
            Arguments.of(new DoubleTestPropertyValues(nodeId -> Math.pow(Math.E, nodeId)), expected)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodePropertyValues properties, double[] expected) {
        var scaler = new LogScaler(properties, 0);

        double[] actual = IntStream.range(1, 5).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

}
