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
import org.neo4j.gds.values.ArrayEquals;
import org.neo4j.gds.values.DoubleArray;
import org.neo4j.gds.values.GdsValue;

import java.util.Arrays;

public class DoubleArrayImpl implements DoubleArray {

    private final double[] value;

    public DoubleArrayImpl(double[] value) {
        this.value = value;
    }

    @Override
    public double[] doubleArrayValue() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public double doubleValue(int idx) {
        return value[idx];
    }

    @Override
    public int length() {
        return value.length;
    }

    @Override
    public ValueType type() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public double[] asObject() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DoubleArray) {
            return equals(((DoubleArray) o).doubleArrayValue());
        } else if (o instanceof GdsValue) {
            return ArrayEquals.doubleAndObject(value, ((GdsValue) o).asObject());
        }
        return false;
    }

    @Override
    public boolean equals(byte[] o) {
        return ArrayEquals.byteAndDouble(o, value);
    }

    @Override
    public boolean equals(short[] o) {
        return ArrayEquals.shortAndDouble(o, value);
    }

    @Override
    public boolean equals(int[] o) {
        return ArrayEquals.intAndDouble(o, value);
    }

    @Override
    public boolean equals(long[] other) {
        return ArrayEquals.longAndDouble(other, value);
    }

    @Override
    public boolean equals(float[] o) {
        return ArrayEquals.floatAndDouble(o, value);
    }

    @Override
    public boolean equals(double[] o) {
        return Arrays.equals(value, o);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "DoubleArray" + Arrays.toString(value);
    }
}
