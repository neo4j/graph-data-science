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
package org.neo4j.gds.values;

import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

/**
 * Compares the elements two {@link Sequence} values hold, for the {@code equals} overloads an {@link Array}
 * and a {@link Vector} implement alike. It takes the backing arrays rather than the values themselves,
 * because a sequence stores its elements in whichever primitive type is narrowest.
 *
 * <p>Handles only {@code a[] == b[]} where {@code type(a) != type(b)}, i.e. {@code byte[] == int[]} and such.
 * Use {@code Arrays.equals()} when both sides have the same type.
 *
 * <p>Each comparison happens in the type of the wider argument, so comparing against a {@code float[]}
 * narrows the other side to {@code float} first and two values that differ only beyond float precision
 * compare equal. Comparison is by {@code !=} rather than {@code compare}, so {@code NaN} never equals
 * {@code NaN} and {@code -0.0} equals {@code 0.0} — both the opposite of what {@code Arrays.equals} does
 * for two arrays of the same type.
 */
public final class SequenceEquals {
    private SequenceEquals() {}

    public static boolean byteAndShort(byte[] a, short[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean byteAndInt(byte[] a, int[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean byteAndLong(byte[] a, long[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean byteAndFloat(byte[] a, float[] b) {
        return sameDoubles(a.length, b.length, i -> (float) a[i], i -> b[i]);
    }

    public static boolean byteAndDouble(byte[] a, double[] b) {
        return sameDoubles(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean shortAndInt(short[] a, int[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean shortAndLong(short[] a, long[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean shortAndFloat(short[] a, float[] b) {
        return sameDoubles(a.length, b.length, i -> (float) a[i], i -> b[i]);
    }

    public static boolean shortAndDouble(short[] a, double[] b) {
        return sameDoubles(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean intAndLong(int[] a, long[] b) {
        return sameLongs(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean intAndFloat(int[] a, float[] b) {
        return sameDoubles(a.length, b.length, i -> (float) a[i], i -> b[i]);
    }

    public static boolean intAndDouble(int[] a, double[] b) {
        return sameDoubles(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean longAndFloat(long[] a, float[] b) {
        return sameDoubles(a.length, b.length, i -> (float) a[i], i -> b[i]);
    }

    public static boolean longAndDouble(long[] a, double[] b) {
        return sameDoubles(a.length, b.length, i -> a[i], i -> b[i]);
    }

    public static boolean floatAndDouble(float[] a, double[] b) {
        return sameDoubles(a.length, b.length, i -> a[i], i -> b[i]);
    }

    private static boolean sameLongs(int lengthA, int lengthB, IntToLongFunction a, IntToLongFunction b) {
        if (lengthA != lengthB) {
            return false;
        }
        for (int i = 0; i < lengthA; i++) {
            if (a.applyAsLong(i) != b.applyAsLong(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameDoubles(int lengthA, int lengthB, IntToDoubleFunction a, IntToDoubleFunction b) {
        if (lengthA != lengthB) {
            return false;
        }
        for (int i = 0; i < lengthA; i++) {
            if (a.applyAsDouble(i) != b.applyAsDouble(i)) {
                return false;
            }
        }
        return true;
    }
}
