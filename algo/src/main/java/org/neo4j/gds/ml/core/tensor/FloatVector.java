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
package org.neo4j.gds.ml.core.tensor;

public class FloatVector {
    private final float[] data;

    public FloatVector(int size) {
        this(new float[size]);
    }

    public FloatVector(float[] data) {
        this.data = data;
    }

    public float[] data() {
        return data;
    }

    public void addMutable(FloatVector other) {
        for (int pos = 0; pos < data.length; pos++) {
            data[pos] += other.data[pos];
        }
    }

    public void scalarMultiply(FloatVector other, float scalar) {
        for (int pos = 0; pos < data.length; pos++) {
            data[pos] = other.data[pos] * scalar;
        }
    }

    public float innerProduct(FloatVector other) {
        float result = 0;
        for (int i = 0; i < data.length; i++) {
            result += data[i] * other.data[i];
        }
        return result;
    }
}
