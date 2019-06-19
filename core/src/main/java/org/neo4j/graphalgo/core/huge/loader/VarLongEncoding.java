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
package org.neo4j.graphalgo.core.huge.loader;

public final class VarLongEncoding {

    static int encodeVLongs(long[] values, int limit, byte[] out, int into) {
        return encodeVLongs(values, 0, limit, out, into);
    }

    static int encodeVLongs(long[] values, int offset, int end, byte[] out, int into) {
        for (int i = offset; i < end; ++i) {
            into = encodeVLong(out, values[i], into);
        }
        return into;
    }

    //@formatter:off
    private static int encodeVLong(final byte[] buffer, final long val, int output) {
        if (val < 128L) {
            buffer[    output] = (byte) (val       | 128L);
            return 1 + output;
        } else if (val < 16384L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 | 128L);
            return 2 + output;
        } else if (val < 2097152L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 | 128L);
            return 3 + output;
        } else if (val < 268435456L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 | 128L);
            return 4 + output;
        } else if (val < 34359738368L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 | 128L);
            return 5 + output;
        } else if (val < 4398046511104L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 | 128L);
            return 6 + output;
        } else if (val < 562949953421312L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 & 127L);
            buffer[6 + output] = (byte) (val >> 42 | 128L);
            return 7 + output;
        } else if (val < 72057594037927936L) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 & 127L);
            buffer[6 + output] = (byte) (val >> 42 & 127L);
            buffer[7 + output] = (byte) (val >> 49 | 128L);
            return 8 + output;
        } else {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 & 127L);
            buffer[6 + output] = (byte) (val >> 42 & 127L);
            buffer[7 + output] = (byte) (val >> 49 & 127L);
            buffer[8 + output] = (byte) (val >> 56 | 128L);
            return 9 + output;
        }
    }

    /**
     * The values are equivalent to:
     *
     * {@code BitUtil.ceilDiv(64 - Long.numberOfLeadingZeros(nodeCount - 1), 7)}
     */
    public static int encodedVLongSize(final long val) {
        if (val < 128L) {
            return 1;
        } else if (val < 16384L) {
            return 2;
        } else if (val < 2097152L) {
            return 3;
        } else if (val < 268435456L) {
            return 4;
        } else if (val < 34359738368L) {
            return 5;
        } else if (val < 4398046511104L) {
            return 6;
        } else if (val < 562949953421312L) {
            return 7;
        } else if (val < 72057594037927936L) {
            return 8;
        } else {
            return 9;
        }
    }

    static long zigZag(final long value) {
        return (value >> 63) ^ (value << 1);
    }

    private VarLongEncoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
