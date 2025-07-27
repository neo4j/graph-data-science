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
package org.neo4j.gds.kmeans;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DistancesTest {

    @Test
    void shouldWorkWithFloatArrayValues(){
        var values = new FloatArrayNodePropertyValues(){

            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{nodeId,nodeId};
            }

            @Override
            public long nodeCount() {
                return 2;
            }
        };
        var distances = new FloatArrayDistances(values);

        assertThat(distances.distance(0,1)).isCloseTo(Math.sqrt(2), Offset.offset(1e-6));

        var floatArrayCoordinate = new FloatArrayCoordinate(2, values);
        floatArrayCoordinate.assign(List.of(1d,1d));

        assertThat(distances.distance(0,floatArrayCoordinate)).isCloseTo(Math.sqrt(2), Offset.offset(1e-6));
        assertThat(distances.distance(1,floatArrayCoordinate)).isCloseTo(0d, Offset.offset(1e-6));

    }

    @Test
    void shouldWorkWithDoubleArrayValues(){
        var values = new DoubleArrayNodePropertyValues(){

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{nodeId,nodeId};
            }

            @Override
            public long nodeCount() {
                return 2;
            }
        };
        var distances = new DoubleArrayDistances(values);

        assertThat(distances.distance(0,1)).isCloseTo(Math.sqrt(2), Offset.offset(1e-6));

        var doubleArrayCoordinate = new DoubleArrayCoordinate(2, values);
        doubleArrayCoordinate.assign(List.of(1d,1d));

        assertThat(distances.distance(0,doubleArrayCoordinate)).isCloseTo(Math.sqrt(2), Offset.offset(1e-6));
        assertThat(distances.distance(1,doubleArrayCoordinate)).isCloseTo(0d, Offset.offset(1e-6));

    }

}
