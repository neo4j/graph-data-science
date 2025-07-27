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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.Arrays;
import java.util.List;

class FloatArrayCoordinate implements Coordinate {

    private final int dimensions;
    private final float[] coordinate;
    private final NodePropertyValues nodePropertyValues;

    FloatArrayCoordinate(int dimensions, NodePropertyValues nodePropertyValues) {
        this.coordinate = new float[dimensions];
        this.nodePropertyValues = nodePropertyValues;
        this.dimensions = dimensions;
    }

    @Override
    public double[] coordinate() {
        var array = new double[dimensions];
        for (int i = 0; i < dimensions; ++i) {
            array[i] = coordinate[i];
        }
        return array;
    }

    @Override
    public void reset() {
        Arrays.fill(coordinate, 0f);
    }

    @Override
    public void assign(long nodeId) {
        var property = nodePropertyValues.floatArrayValue(nodeId);
        System.arraycopy(property, 0, coordinate, 0, dimensions);
    }

    @Override
    public void assign(List<Double> coordinate) {
        int index = 0;
        for (double value : coordinate) {
            this.coordinate[index++] = (float) value;
        }
    }

    @Override
    public void normalize(long length) {
        for (int i = 0; i < dimensions; ++i) {
            coordinate[i] /= (float) length;
        }
    }

    @Override
    public void add(Coordinate externalCoordinate) {
        var floatArrayCoordinate = (FloatArrayCoordinate) externalCoordinate;
        for (int i = 0; i < dimensions; ++i) {
            coordinate[i] += floatArrayCoordinate.valueAt(i);
        }
    }

    @Override
    public void addTo(long nodeId) {
        var property = nodePropertyValues.floatArrayValue(nodeId);
        for (int i = 0; i < dimensions; ++i) {
            coordinate[i] += property[i];
        }
    }

    private float valueAt(int index) {
        return coordinate[index];
    }

    float[] floatCoordinate() {
        return coordinate;
    }
}
