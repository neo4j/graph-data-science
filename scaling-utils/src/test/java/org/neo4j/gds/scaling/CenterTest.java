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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.nodeproperties.DoubleTestPropertyValues;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CenterTest {

    private static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of(
                new DoubleTestPropertyValues(nodeId -> nodeId),
                4.5,
                new double[]{-4.5, -3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5, 4.5}
            ),
            Arguments.of(
                new DoubleTestPropertyValues(nodeId -> -nodeId),
                -4.5,
                new double[]{4.5, 3.5, 2.5, 1.5, 0.5, -0.5, -1.5, -2.5, -3.5, -4.5}
            )

        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodePropertyValues properties, double avg, double[] expected) {
        var scaler = (Center) Center.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        assertThat(scaler.avg).isEqualTo(avg);
        assertThat(scaler.statistics()).containsExactlyEntriesOf(Map.of("avg", List.of(avg)));

        double[] actual = IntStream.range(0, 10).mapToDouble(scaler::scaleProperty).toArray();
        assertThat(actual).containsSequence(expected);
    }

    @Test
    void handlesMissingValue() {
        var properties = new DoubleTestPropertyValues(value -> value == 5 ? Double.NaN : value);
        var scaler = Center.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        var avg = 4.444;
        for (int i = 0; i < 5; i++) {
            assertThat(scaler.scaleProperty(i)).isCloseTo(i - avg, Offset.offset(1e-3));
        }
        assertThat(scaler.scaleProperty(5)).isNaN();
        for (int i = 6; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isCloseTo(i - avg, Offset.offset(1e-3));
        }
    }


}
