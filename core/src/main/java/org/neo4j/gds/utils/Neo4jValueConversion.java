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
package org.neo4j.gds.utils;

import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongArray;
import org.neo4j.values.storable.ShortArray;
import org.neo4j.values.storable.Value;

import java.util.Locale;
import java.util.function.IntToLongFunction;

import static org.neo4j.gds.api.ValueConversion.exactDoubleToLong;
import static org.neo4j.gds.api.ValueConversion.exactLongToDouble;
import static org.neo4j.gds.api.ValueConversion.exactLongToFloat;
import static org.neo4j.gds.api.ValueConversion.notOverflowingDoubleToFloat;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class Neo4jValueConversion {

    public static long getLongValue(Value value) {
        if (value instanceof IntegralValue) {
            return ((IntegralValue) value).longValue();
        } else if (value instanceof FloatingPointValue) {
            return exactDoubleToLong(((FloatingPointValue) value).doubleValue());
        } else {
            throw conversionError(value, "Long");
        }
    }

    public static double getDoubleValue(Value value) {
        if (value instanceof FloatingPointValue) {
            return ((FloatingPointValue) value).doubleValue();
        } else if (value instanceof IntegralValue) {
            return exactLongToDouble(((IntegralValue) value).longValue());
        } else {
            throw conversionError(value, "Double");
        }
    }

    public static long[] getLongArray(Value value) {
        if (value instanceof LongArray) {
            return ((LongArray) value).asObjectCopy();
        } else if (value instanceof FloatingPointArray) {
            return floatToLongArray((FloatingPointArray) value);
        }
        else {
            throw conversionError(value, "Long Array");
        }
    }

    public static double[] getDoubleArray(Value value) {
        if (value instanceof DoubleArray) {
            return ((DoubleArray) value).asObjectCopy();
        } else if (value instanceof FloatArray) {
            return floatToDoubleArray((FloatArray) value);
        } else if (value instanceof IntegralArray) {
            return integralToDoubleArray((IntegralArray) value);
        } else {
            throw conversionError(value, "Double Array");
        }
    }

    public static float[] getFloatArray(Value value) {
        if (value instanceof FloatArray) {
            return ((FloatArray) value).asObjectCopy();
        } else if (value instanceof DoubleArray) {
            return doubleToFloatArray((DoubleArray) value);
        } else if (value instanceof IntegralArray) {
            return longToFloatArray((IntegralArray) value);
        }else {
            throw conversionError(value, "Float Array");
        }
    }

    private static double[] integralToDoubleArray(IntegralArray intArray) {
        var result = new double[intArray.length()];

        IntToLongFunction longValueProvider = resolvelongValueProvider(intArray);

        try {
            for (int idx = 0; idx < intArray.length(); idx++) {
                result[idx] = exactLongToDouble(longValueProvider.applyAsLong(idx));
            }
        } catch (UnsupportedOperationException e) {
            throw conversionError(intArray, "Double Array", e.getMessage());
        }

        return result;
    }

    private static double[] floatToDoubleArray(FloatArray floatArray) {
        var result = new double[floatArray.length()];

        for (int idx = 0; idx < floatArray.length(); idx++) {
            result[idx] = floatArray.doubleValue(idx);
        }

        return result;
    }

    private static float[] doubleToFloatArray(DoubleArray doubleArray) {
        var result = new float[doubleArray.length()];

        try {
            for (int idx = 0; idx < doubleArray.length(); idx++) {
                result[idx] = notOverflowingDoubleToFloat(doubleArray.doubleValue(idx));
            }
        } catch (UnsupportedOperationException e) {
            throw conversionError(doubleArray, "Float Array", e.getMessage());
        }

        return result;
    }

    private static float[] longToFloatArray(IntegralArray integralArray) {
        var result = new float[integralArray.length()];

        IntToLongFunction longValueProvider = resolvelongValueProvider(integralArray);

        try {
            for (int idx = 0; idx < integralArray.length(); idx++) {
                result[idx] = exactLongToFloat(longValueProvider.applyAsLong(idx));
            }
        } catch (UnsupportedOperationException e) {
            throw conversionError(integralArray, "Float Array", e.getMessage());
        }

        return result;
    }

    private static IntToLongFunction resolvelongValueProvider(IntegralArray integralArray) {
        // need narrow casting as IntegralArray::longValue is only public since Neo4j 5.4
        if (integralArray instanceof LongArray) {
            return ((LongArray) integralArray)::longValue;
        } else if (integralArray instanceof IntArray) {
            return ((IntArray) integralArray)::longValue;
        } else if (integralArray instanceof ShortArray) {
            return ((ShortArray) integralArray)::longValue;
        } else if (integralArray instanceof ByteArray) {
            return ((ByteArray) integralArray)::longValue;
        }

        throw new IllegalStateException(String.format(
            Locale.US,
            "Did not expect array of type %s.", integralArray.getClass().getSimpleName()
        ));
    }

    private static long[] floatToLongArray(FloatingPointArray floatArray) {
        var result = new long[floatArray.length()];

        try {
            for (int idx = 0; idx < floatArray.length(); idx++) {
                result[idx] = exactDoubleToLong(floatArray.doubleValue(idx));
            }
        } catch (UnsupportedOperationException e) {
            throw conversionError(floatArray, "Long Array", e.getMessage());
        }

        return result;
    }

    private static UnsupportedOperationException conversionError(Value value, String expected) {
        return conversionError(value, expected, "");
    }

    private static UnsupportedOperationException conversionError(Value value, String expected, String context) {
        return new UnsupportedOperationException(formatWithLocale(
            "Cannot safely convert %s into a %s. %s",
            value,
            expected,
            context
        ));
    }

    private Neo4jValueConversion() {}
}
