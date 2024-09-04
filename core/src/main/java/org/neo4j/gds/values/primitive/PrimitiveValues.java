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

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.values.Array;
import org.neo4j.gds.values.DoubleArray;
import org.neo4j.gds.values.FloatArray;
import org.neo4j.gds.values.FloatingPointValue;
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.IntegralValue;
import org.neo4j.gds.values.LongArray;

import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PrimitiveValues {
    public static final GdsNoValue NO_VALUE = GdsNoValue.NO_VALUE;

    public static GdsValue of(@Nullable Object value) {
        GdsValue of = unsafeOf(value);
        if (of != null) {
            return of;
        }
        Objects.requireNonNull(value);
        throw new IllegalArgumentException(formatWithLocale("[%s:%s] is not a supported property value", value, value.getClass().getName()));
    }

    private static @Nullable GdsValue unsafeOf(Object value) {
        if (value == null) return NO_VALUE;
        if (value instanceof Number) {
            return numberValue((Number) value);
        }
        if (value instanceof Object[]) {
            return arrayValue((Object[]) value);
        }
        if (value instanceof byte[]) {
            return byteArray((byte[]) value);
        }
        if (value instanceof short[]) {
            return shortArray((short[]) value);
        }
        if (value instanceof int[]) {
            return intArray((int[]) value);
        }
        if (value instanceof long[]) {
            return longArray((long[]) value);
        }
        if (value instanceof float[]) {
            return floatArray((float[]) value);
        }
        if (value instanceof double[]) {
            return doubleArray((double[]) value);
        }
        return null;
    }

    private static GdsValue numberValue(Number number) {
        if (number instanceof Long longNumber) {
            return longValue(longNumber);
        } else if (number instanceof Integer intNumber) {
            return longValue(intNumber);
        } else if (number instanceof Double doubleNumber) {
            return floatingPointValue(doubleNumber);
        } else if (number instanceof Byte byteNumber) {
            return longValue(byteNumber);
        } else if (number instanceof Float floatNumber) {
            return floatingPointValue(floatNumber);
        } else if (number instanceof Short shortNumber) {
            return longValue(shortNumber);
        } else {
            throw new UnsupportedOperationException("Unsupported type of Number " + number);
        }
    }

    private static @Nullable Array arrayValue(Object[] value) {
        if (value instanceof Float[]) {
            return floatArray(copy(value, new float[value.length]));
        }
        if (value instanceof Double[]) {
            return doubleArray(copy(value, new double[value.length]));
        }
        if (value instanceof Long[]) {
            return longArray(copy(value, new long[value.length]));
        }
        if (value instanceof Integer[]) {
            return intArray(copy(value, new int[value.length]));
        }
        if (value instanceof Short[]) {
            return shortArray(copy(value, new short[value.length]));
        }
        if (value instanceof Byte[]) {
            return byteArray(copy(value, new byte[value.length]));
        }
        return null;
    }


    public static IntegralValue longValue(long value) {
        return new LongValueImpl(value);
    }
    public static FloatingPointValue floatingPointValue(double value) {
        return new FloatingPointValueImpl(value);
    }

    public static DoubleArray doubleArray(double[] data) {
        return new DoubleArrayImpl(data);
    }
    public static FloatArray floatArray(float[] data) {
        return new FloatArrayImpl(data);
    }
    public static LongArray longArray(long[] data) {
        return new LongArrayImpl(data);
    }
    public static LongArray intArray(int[] data) {
        return new IntLongArrayImpl(data);
    }
    public static LongArray shortArray(short[] data) {
        return new ShortLongArrayImpl(data);
    }
    public static LongArray byteArray(byte[] data) {
        return new ByteLongArrayImpl(data);
    }

    private static <T> T copy(Object[] value, T target) {
        for(int i = 0; i < value.length; ++i) {
            if (value[i] == null) {
                throw new IllegalArgumentException("Property array value elements may not be null.");
            }
            java.lang.reflect.Array.set(target, i, value[i]);
        }
        return target;
    }

    private PrimitiveValues() {}

}
