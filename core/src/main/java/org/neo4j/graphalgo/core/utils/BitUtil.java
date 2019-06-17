/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

public final class BitUtil {

    public static boolean isPowerOfTwo(final int value) {
        return value > 0 && ((value & (~value + 1)) == value);
    }

    public static boolean isPowerOfTwo(final long value) {
        return value > 0L && ((value & (~value + 1L)) == value);
    }

    /**
     * returns the previous highest power of two, or the current value if it's already a power of two or zero
     */
    public static int previousPowerOfTwo(int v) {
        v |= (v >>  1);
        v |= (v >>  2);
        v |= (v >>  4);
        v |= (v >>  8);
        v |= (v >> 16);
        return v - (v >>> 1);
    }

    /**
     * returns the previous highest power of two, or the current value if it's already a power of two or zero
     */
    public static long previousPowerOfTwo(long v) {
        v |= (v >>  1L);
        v |= (v >>  2L);
        v |= (v >>  4L);
        v |= (v >>  8L);
        v |= (v >> 16L);
        v |= (v >> 32L);
        return v - (v >>> 1L);
    }

    public static int nearbyPowerOfTwo(int x) {
        int next = nextHighestPowerOfTwo(x);
        int prev = next >>> 1;
        return (next - x) <= (x - prev) ? next : prev;
    }

    public static long nearbyPowerOfTwo(long x) {
        long next = nextHighestPowerOfTwo(x);
        long prev = next >>> 1;
        return (next - x) <= (x - prev) ? next : prev;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static long nextHighestPowerOfTwo(long v) {
        v--;
        v |= v >> 1L;
        v |= v >> 2L;
        v |= v >> 4L;
        v |= v >> 8L;
        v |= v >> 16L;
        v |= v >> 32L;
        v++;
        return v;
    }

    public static long align(long value, int alignment) {
        assert isPowerOfTwo(alignment) : "alignment must be a power of 2:" + alignment;
        return value + (long) (alignment - 1) & (long) (~(alignment - 1));
    }

    public static long ceilDiv(long dividend, long divisor) {
        return 1L + (-1L + dividend) / divisor;
    }

    private BitUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
