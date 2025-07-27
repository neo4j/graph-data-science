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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.FloatArrayNodePropertyValues;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


class CoordinateTest {

    @Test
    void shouldWorkWithFloatArrays(){
        var values = new FloatArrayNodePropertyValues(){

            @Override
            public float[] floatArrayValue(long nodeId) {
                return new float[]{nodeId,nodeId};
            }

            @Override
            public long nodeCount() {
                return 5;
            }
        };

        var coordinate = new FloatArrayCoordinate(2,values);
        coordinate.assign(List.of(1d,1d));
        assertThat(coordinate.coordinate()).containsExactly(1d,1d);
        coordinate.reset();
        assertThat(coordinate.coordinate()).containsExactly(0d,0d);
        coordinate.assign(3);
        assertThat(coordinate.coordinate()).containsExactly(3d,3d);
        coordinate.addTo(1);
        assertThat(coordinate.coordinate()).containsExactly(4d,4d);
        var toAdd = new FloatArrayCoordinate(2,values);
        toAdd.assign(2);
        coordinate.add(toAdd);
        assertThat(coordinate.coordinate()).containsExactly(6d,6d);
        coordinate.normalize(3);
        assertThat(coordinate.coordinate()).containsExactly(2d,2d);

    }

    @Test
    void shouldWorkWithDoubleArrays(){
        var values = new DoubleArrayNodePropertyValues(){

            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{nodeId,nodeId};
            }

            @Override
            public long nodeCount() {
                return 5;
            }
        };

        var coordinate = new DoubleArrayCoordinate(2,values);
        coordinate.assign(List.of(1d,1d));
        assertThat(coordinate.coordinate()).containsExactly(1d,1d);
        coordinate.reset();
        assertThat(coordinate.coordinate()).containsExactly(0d,0d);
        coordinate.assign(3);
        assertThat(coordinate.coordinate()).containsExactly(3d,3d);
        coordinate.addTo(1);
        assertThat(coordinate.coordinate()).containsExactly(4d,4d);
        var toAdd = new DoubleArrayCoordinate(2,values);
        toAdd.assign(2);
        coordinate.add(toAdd);
        assertThat(coordinate.coordinate()).containsExactly(6d,6d);
        coordinate.normalize(3);
        assertThat(coordinate.coordinate()).containsExactly(2d,2d);
    }

    @Test
    void shouldWorkWithScalars(){
        var values = new DoubleNodePropertyValues(){


            @Override
            public double doubleValue(long nodeId) {
                return nodeId;
            }

            @Override
            public long nodeCount() {
                return 5;
            }
        };

        var coordinate = new ScalarCoordinate(values);
        coordinate.assign(List.of(1d));
        assertThat(coordinate.coordinate()).containsExactly(1d);
        coordinate.reset();
        assertThat(coordinate.coordinate()).containsExactly(0d);
        coordinate.assign(3);
        assertThat(coordinate.coordinate()).containsExactly(3d);
        coordinate.addTo(1);
        assertThat(coordinate.coordinate()).containsExactly(4d);
        var toAdd = new ScalarCoordinate(values);
        toAdd.assign(2);
        coordinate.add(toAdd);
        assertThat(coordinate.coordinate()).containsExactly(6d);
        coordinate.normalize(3);
        assertThat(coordinate.coordinate()).containsExactly(2d);
    }
}
