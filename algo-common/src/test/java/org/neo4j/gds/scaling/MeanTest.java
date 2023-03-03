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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MeanTest {

    private static Stream<Arguments> properties() {
        double[] expected = {-0.5, -7 / 18D, -5 / 18D, -3 / 18D, -1 / 18D, 1 / 18D, 3 / 18D, 5 / 18D, 7 / 18D, 0.5};
        return Stream.of(
            Arguments.of(new DoubleTestPropertyValues(nodeId -> nodeId), 4.5D, 0D, 9D, expected),
            Arguments.of(new DoubleTestPropertyValues(nodeId -> -nodeId - 1), -5.5D, -10D, -1D, Arrays.stream(expected).map(v -> -v).toArray()),
            Arguments.of(new DoubleTestPropertyValues(nodeId -> 50000000D * nodeId), 50000000D * 4.5D, 0D, 4.5e8, expected)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodePropertyValues properties, double avg, double min, double max, double[] expected) {
        var scaler = (Mean) Mean.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(scaler.avg).isEqualTo(avg);
        assertThat(scaler.maxMinDiff).isEqualTo(max - min);
        assertThat(scaler.statistics()).containsExactlyEntriesOf(Map.of(
            "max", List.of(max),
            "avg", List.of(avg),
            "min", List.of(min)
        ));

        double[] actual = IntStream.range(0, 10).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void avoidsDivByZero() {
        double propValue = 4D;
        var properties = new DoubleTestPropertyValues(nodeId -> propValue);
        var scaler = Mean.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(scaler.statistics()).containsExactlyEntriesOf(Map.of(
            "max", List.of(propValue),
            "avg", List.of(Double.POSITIVE_INFINITY),
            "min", List.of(propValue)
        ));

        for (int i = 0; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isEqualTo(0D);
        }
    }

}
