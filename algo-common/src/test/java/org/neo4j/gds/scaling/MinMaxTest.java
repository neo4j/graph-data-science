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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class MinMaxTest {

    private static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of(new DoubleTestPropertyValues(nodeId -> nodeId), 0D, 9D),
            Arguments.of(new DoubleTestPropertyValues(nodeId -> 50000000D * nodeId), 0D, 4.5e8)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void normalizes(NodePropertyValues properties, double min, double max) {
        var scaler = (MinMax) MinMax.buildFrom(CypherMapWrapper.empty()).create(
            properties,
            10,
            1,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(scaler.min).isEqualTo(min);
        assertThat(scaler.maxMinDiff).isEqualTo(max - min);
        assertThat(scaler.statistics()).containsExactlyEntriesOf(Map.of(
            "max", List.of(max),
            "min", List.of(min)
        ));

        for (int i = 0; i < 10; i++) {
            assertThat(scaler.scaleProperty(i)).isEqualTo(i / 9D);
        }
    }

    @Test
    void avoidsDivByZero() {
        var properties = new DoubleTestPropertyValues(nodeId -> 4D);
        var scaler = MinMax.buildFrom(CypherMapWrapper.empty()).create(
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
