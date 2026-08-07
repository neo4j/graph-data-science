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
package org.neo4j.gds.values.primitive;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.values.FloatVector;
import org.neo4j.gds.values.Sequence;
import org.neo4j.gds.values.SequenceEquals;

import java.util.Arrays;

public class FloatVectorImpl implements FloatVector {

    private final float[] value;

    public FloatVectorImpl(float[] value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.FLOAT_VECTOR;
    }

    @Override
    public float[] floatVectorValue() {
        var copy = new float[value.length];
        System.arraycopy(value, 0, copy, 0, value.length);
        return copy;
    }

    @Override
    public float floatValue(int idx) {
        return value[idx];
    }

    @Override
    public int dimension() {
        return value.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Sequence other) {
            return other.equals(value);
        }
        return false;
    }

    @Override
    public boolean equals(byte[] o) {
        return SequenceEquals.byteAndFloat(o, value);
    }

    @Override
    public boolean equals(short[] o) {
        return SequenceEquals.shortAndFloat(o, value);
    }

    @Override
    public boolean equals(int[] o) {
        return SequenceEquals.intAndFloat(o, value);
    }

    @Override
    public boolean equals(long[] other) {
        return SequenceEquals.longAndFloat(other, value);
    }

    @Override
    public boolean equals(float[] o) {
        return Arrays.equals(value, o);
    }

    @Override
    public boolean equals(double[] o) {
        return SequenceEquals.floatAndDouble(value, o);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "FloatVector" + Arrays.toString(value);
    }
}
