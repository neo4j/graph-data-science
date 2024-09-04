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
import org.neo4j.gds.values.FloatArray;
import org.neo4j.gds.values.GdsValue;

import java.util.Arrays;

public class FloatArrayImpl implements FloatArray {

    private final float[] value;

    public FloatArrayImpl(float[] value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.FLOAT_ARRAY;
    }

    @Override
    public double[] asObject() {
        return doubleArrayValue();
    }

    @Override
    public double[] doubleArrayValue() {
        var copy = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            copy[i] = value[i];
        }
        return copy;
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
    public float[] floatArrayValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof FloatArray) {
            return equals(((FloatArray) o).floatArrayValue());
        } else if (o instanceof GdsValue) {
            return ArrayEquals.floatAndObject(value, ((GdsValue) o).asObject());
        }
        return false;
    }

    public boolean equals(float[] o) {
        return Arrays.equals(value, o);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
