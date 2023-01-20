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
package org.neo4j.gds.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;
import java.util.Objects;

import static org.neo4j.gds.api.DefaultValueUtil.parseDoubleArrayValue;
import static org.neo4j.gds.api.DefaultValueUtil.parseFloatArrayValue;
import static org.neo4j.gds.api.DefaultValueUtil.parseLongArrayValue;
import static org.neo4j.gds.api.DefaultValueUtil.transformObjectToPrimitiveArray;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class DefaultValue {
    public static final DefaultValue DEFAULT = DefaultValue.ofFallBackValue(null);
    public static final int INTEGER_DEFAULT_FALLBACK = Integer.MIN_VALUE;
    public static final long LONG_DEFAULT_FALLBACK = Long.MIN_VALUE;
    public static final float FLOAT_DEFAULT_FALLBACK = Float.NaN;
    public static final double DOUBLE_DEFAULT_FALLBACK = Double.NaN;
    private static final double[] DOUBLE_ARRAY_DEFAULT_FALLBACK = DEFAULT.doubleArrayValue();
    private static final long[] LONG_ARRAY_DEFAULT_FALLBACK = DEFAULT.longArrayValue();
    private static final float[] FLOAT_ARRAY_DEFAULT_FALLBACK = DEFAULT.floatArrayValue();

    @Nullable
    private final Object defaultValue;

    private final boolean isUserDefined;

    public static DefaultValue of(Object defaultValue) {
        return of(defaultValue, true);
    }

    public static DefaultValue of(@Nullable Object defaultValue, boolean isUserDefined) {
        if (defaultValue instanceof DefaultValue) {
            return (DefaultValue) defaultValue;
        } else {
            if (defaultValue instanceof List) {
                var primitiveArray = transformObjectToPrimitiveArray(((List<?>) defaultValue).toArray());
                return new DefaultValue(primitiveArray, isUserDefined);
            }

            return new DefaultValue(defaultValue, isUserDefined);
        }
    }

    public static DefaultValue of(@Nullable Object defaultValue, ValueType type, boolean isUserDefined) {
        if (defaultValue == null || defaultValue.toString().isBlank()) {
            return type.fallbackValue();
        }
        switch (type) {
            case LONG:
                return DefaultValue.of(Long.parseLong(defaultValue.toString()), isUserDefined);
            case DOUBLE:
                return DefaultValue.of(Double.parseDouble(defaultValue.toString()), isUserDefined);
            case DOUBLE_ARRAY:
                return DefaultValue.of(parseDoubleArrayValue(defaultValue, type), isUserDefined);
            case LONG_ARRAY:
                return DefaultValue.of(parseLongArrayValue(defaultValue, type), isUserDefined);
            case FLOAT_ARRAY:
                return DefaultValue.of(parseFloatArrayValue(defaultValue, type), isUserDefined);
            default:
                return DefaultValue.of(defaultValue, isUserDefined);
        }
    }

    private static DefaultValue ofFallBackValue(@Nullable Object defaultValue) {
        return of(defaultValue, false);
    }

    public static DefaultValue forInt() {
        return DefaultValue.ofFallBackValue(INTEGER_DEFAULT_FALLBACK);
    }

    public static DefaultValue forLong() {
        return DefaultValue.ofFallBackValue(LONG_DEFAULT_FALLBACK);
    }

    public static DefaultValue forDouble() {
        return DefaultValue.ofFallBackValue(DOUBLE_DEFAULT_FALLBACK);
    }

    public static DefaultValue forFloat() {
        return DefaultValue.ofFallBackValue(FLOAT_DEFAULT_FALLBACK);
    }

    public static DefaultValue forLongArray() {
        return DefaultValue.ofFallBackValue(LONG_ARRAY_DEFAULT_FALLBACK);
    }

    public static DefaultValue forFloatArray() {
        return DefaultValue.ofFallBackValue(FLOAT_ARRAY_DEFAULT_FALLBACK);
    }

    public static DefaultValue forDoubleArray() {
        return DefaultValue.ofFallBackValue(DOUBLE_ARRAY_DEFAULT_FALLBACK);
    }

    private DefaultValue(@Nullable Object defaultValue, boolean isUserDefined) {
        this.defaultValue = defaultValue;
        this.isUserDefined = isUserDefined;
    }

    public boolean isUserDefined() {
        return isUserDefined;
    }

    public long longValue() {
        if (defaultValue == null) {
            return LONG_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Double && Double.isNaN((double) defaultValue)) {
            return LONG_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Float && Float.isNaN((float) defaultValue)) {
            return LONG_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Double || defaultValue instanceof Float) {
            return ValueConversion.exactDoubleToLong(((Number) defaultValue).doubleValue());
        } else if (defaultValue instanceof Number) {
            return ((Number) defaultValue).longValue();
        }
        throw getInvalidTypeException(Long.class);
    }

    public double doubleValue() {
        if (defaultValue instanceof Long && defaultValue.equals(LONG_DEFAULT_FALLBACK)) {
            return DOUBLE_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Long || defaultValue instanceof Integer) {
            return ValueConversion.exactLongToDouble((((Number) defaultValue).longValue()));
        } else if (defaultValue instanceof Number) {
            return ((Number) defaultValue).doubleValue();
        } else if (defaultValue == null) {
            return DOUBLE_DEFAULT_FALLBACK;
        }
        throw getInvalidTypeException(Double.class);
    }

    public float[] floatArrayValue() {
        float[] floatArray = null;

        if (defaultValue == null) {
            return null;
        } else if (defaultValue instanceof float[]) {
            return (float[]) defaultValue;
        } else if (defaultValue instanceof double[]) {
            var doubleArray = (double[]) defaultValue;
            floatArray = new float[doubleArray.length];

            for (int i = 0; i < doubleArray.length; i++) {
                floatArray[i] = ValueConversion.notOverflowingDoubleToFloat(doubleArray[i]);
            }
        } else if (defaultValue instanceof long[]) {
            var longArray = (long[]) defaultValue;
            floatArray = new float[longArray.length];

            for (int i = 0; i < longArray.length; i++) {
                floatArray[i] = ValueConversion.exactLongToFloat(longArray[i]);
            }
        } else {
            throw getInvalidTypeException(float[].class);
        }

        return floatArray;
    }

    public double[] doubleArrayValue() {
        double[] doubleArray = null;

        if (defaultValue == null) {
            return null;
        } else if (defaultValue instanceof double[]) {
            return (double[]) defaultValue;
        } else if (defaultValue instanceof float[]) {
            var floatArray = (float[]) defaultValue;
            doubleArray = new double[floatArray.length];

            for (int i = 0; i < floatArray.length; i++) {
                doubleArray[i] = floatArray[i];
            }
        } else if (defaultValue instanceof long[]) {
            var longArray = (long[]) defaultValue;
            doubleArray = new double[longArray.length];

            for (int i = 0; i < longArray.length; i++) {
                doubleArray[i] = ValueConversion.exactLongToDouble(longArray[i]);
            }
        } else {
            throw getInvalidTypeException(double[].class);
        }

        return doubleArray;
    }

    public long[] longArrayValue() {
        long[] longArray = null;

        if (defaultValue == null) {
            return null;
        } else if (defaultValue instanceof long[]) {
            return (long[]) defaultValue;
        } else if (defaultValue instanceof float[]) {
            var floatArray = (float[]) defaultValue;
            longArray = new long[floatArray.length];

            for (int i = 0; i < floatArray.length; i++) {
                longArray[i] = ValueConversion.exactDoubleToLong(floatArray[i]);
            }
        } else if (defaultValue instanceof double[]) {
            var doubleArray = (double[]) defaultValue;
            longArray = new long[doubleArray.length];

            for (int i = 0; i < doubleArray.length; i++) {
                longArray[i] = ValueConversion.exactDoubleToLong(doubleArray[i]);
            }
        } else {
            throw getInvalidTypeException(long[].class);
        }

        return longArray;
    }

    public @Nullable Object getObject() {
        return defaultValue;
    }


    @Override
    public String toString() {
        return "DefaultValue(" + defaultValue + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultValue that = (DefaultValue) o;
        return Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultValue);
    }

    @NotNull
    private IllegalArgumentException getInvalidTypeException(Class<?> expectedClass) {
        return new IllegalArgumentException(formatWithLocale(
            "Expected type of default value to be `%s`. But got `%s`.",
            expectedClass.getSimpleName(),
            defaultValue.getClass().getSimpleName()
        ));
    }

}
