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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class L2NormTest {

    private static Stream<Arguments> properties() {
        double l2norm = 16.881943016134134;
        double[] expected = {0 / l2norm, 1 / l2norm, 2 / l2norm, 3 / l2norm, 4 / l2norm, 5 / l2norm, 6 / l2norm, 7 / l2norm, 8 / l2norm, 9 / l2norm};
        return Stream.of(
            Arguments.of(new DoubleTestPropertyValues(nodeId -> nodeId), l2norm, expected)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodePropertyValues properties, double euclideanLength, double[] expected) {
        var scaler = (L2Norm) L2Norm.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(scaler.euclideanLength).isEqualTo(euclideanLength);

        double[] actual = IntStream.range(0, 10).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void avoidsDivByZero() {
        var properties = new DoubleTestPropertyValues(nodeId -> 0D);
        var scaler = L2Norm.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        for (int i = 0; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isEqualTo(0D);
        }
    }
}
