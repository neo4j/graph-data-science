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
package org.neo4j.gds.hdbscan;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FloatDistancesTest {

    @Nested
    class AABBLowerBoundTest {


        @Test
        void lowerBoundOfLookupIsInsideTheBox() {
            var aabb = new AABB(
                new double[]{2.0f, 1.0f},
                new double[]{9.0f, 7.0f},
                2
            );

            var nodeProps = mock(FloatArrayNodePropertyValues.class);
            when(nodeProps.floatArrayValue(anyLong())).thenReturn(new float[]{3.9f, 1.2f});

            var distances = new FloatArrayDistances(nodeProps);

            assertThat(distances.lowerBound(aabb, 0)).isZero();
        }

        @Test
        void lowerBoundOfLookupIsOutsideTheBoxTopRight() {
            var aabb = new AABB(
                new double[]{2.0f, 1.0f},
                new double[]{9.0f, 7.0f},
                2
            );

            var nodeProps = mock(FloatArrayNodePropertyValues.class);
            when(nodeProps.floatArrayValue(anyLong())).thenReturn(new float[]{11.0f, 0.2f});
            var distances = new FloatArrayDistances(nodeProps);

            double lowerBound = distances.lowerBound(aabb, 0);

            assertThat(lowerBound).isCloseTo(2.1540659229, Offset.offset(1e-5));
        }

        @Test
        void lowerBoundOfLookupIsOutsideTheBoxBottom() {
            var aabb = new AABB(
                new double[]{2.0f, 1.0f},
                new double[]{9.0f, 7.0f},
                2
            );

            var nodeProps = mock(FloatArrayNodePropertyValues.class);
            when(nodeProps.floatArrayValue(anyLong())).thenReturn(new float[]{8.0f, 8.0f});
            var distances = new FloatArrayDistances(nodeProps);

            double lowerBound = distances.lowerBound(aabb, 0);

            assertThat(lowerBound).isEqualTo(1d);
        }

        @Test
        void lowerBoundOfLookupIsAtTheEdgeOfTheBox() {
            var aabb = new AABB(
                new double[]{2.0f, 1.0f},
                new double[]{9.0f, 7.0f},
                2
            );

            var nodeProps = mock(FloatArrayNodePropertyValues.class);
            when(nodeProps.floatArrayValue(anyLong())).thenReturn(new float[]{9.0f, 1.0f});
            var distances = new FloatArrayDistances(nodeProps);

            double lowerBound = distances.lowerBound(aabb, 0);
            assertThat(lowerBound).isZero();
        }
    }

}
