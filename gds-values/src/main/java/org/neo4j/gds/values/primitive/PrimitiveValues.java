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
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.values.Array;
import org.neo4j.gds.values.DoubleArray;
import org.neo4j.gds.values.DoubleVector;
import org.neo4j.gds.values.FloatArray;
import org.neo4j.gds.values.FloatVector;
import org.neo4j.gds.values.FloatingPointValue;
import org.neo4j.gds.values.GdsNoValue;
import org.neo4j.gds.values.GdsValue;
import org.neo4j.gds.values.IntegralValue;
import org.neo4j.gds.values.LongArray;

import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

public final class PrimitiveValues {
    private static final long[] EMPTY_LONGS = new long[0];
    public static final GdsNoValue NO_VALUE = GdsNoValue.NO_VALUE;
    public static final LongArray EMPTY_LONG_ARRAY = longArray(EMPTY_LONGS);

    public static Function<Object, GdsValue> valueCreator(ValueType valueType) {
        Function<Object, GdsValue> valueCreator = switch (valueType) {
            case LONG -> l -> longValue((long) l);
            case DOUBLE -> d -> floatingPointValue((double) d);
            case DOUBLE_ARRAY -> da -> doubleArray((double[]) da);
            case FLOAT_ARRAY -> fa -> floatArray((float[]) fa);
            case LONG_ARRAY -> la -> longArray((long[]) la);
            case FLOAT_VECTOR -> fv -> floatVector((float[]) fv);
            case DOUBLE_VECTOR -> dv -> doubleVector((double[]) dv);
            case STRING, UNKNOWN, UNTYPED_ARRAY -> PrimitiveValues::create;
        };

        return value ->  value != null ? valueCreator.apply(value) : null;
    }

    public static GdsValue create(@Nullable Object value) {
        GdsValue of = of(value);
        if (of != null) {
            return of;
        }
        Objects.requireNonNull(value);
        throw new IllegalArgumentException(
            String.format(
                Locale.ENGLISH,
                "[%s:%s] is not a supported property value", value, value.getClass().getName()
            )
        );
    }

    private static @Nullable GdsValue of(Object value) {
        return switch (value) {
            case null -> NO_VALUE;
            case Number number -> numberValue(number);
            case Object[] objects -> arrayValue(objects);
            case byte[] bytes -> byteArray(bytes);
            case short[] shorts -> shortArray(shorts);
            case int[] ints -> intArray(ints);
            case long[] longs -> longArray(longs);
            case float[] floats -> floatArray(floats);
            case double[] doubles -> doubleArray(doubles);
            default -> null;
        };
    }

    private static GdsValue numberValue(Number number) {
        return switch (number) {
            case Long longNumber -> longValue(longNumber);
            case Integer intNumber -> longValue(intNumber);
            case Double doubleNumber -> floatingPointValue(doubleNumber);
            case Byte byteNumber -> longValue(byteNumber);
            case Float floatNumber -> floatingPointValue(floatNumber);
            case Short shortNumber -> longValue(shortNumber);
            case null, default -> throw new UnsupportedOperationException("Unsupported type of Number " + number);
        };
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
    public static FloatVector floatVector(float[] data) {
        return new FloatVectorImpl(data);
    }
    public static DoubleVector doubleVector(double[] data) {
        return new DoubleVectorImpl(data);
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
