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
package org.neo4j.gds.core.loading;

import org.neo4j.internal.unsafe.UnsafeUtil;

public final class VarLongEncoding {

    public static final long THRESHOLD_1_BYTE = 128L;
    public static final long THRESHOLD_2_BYTE = 16384L;
    public static final long THRESHOLD_3_BYTE = 2097152L;
    public static final long THRESHOLD_4_BYTE = 268435456L;
    public static final long THRESHOLD_5_BYTE = 34359738368L;
    public static final long THRESHOLD_6_BYTE = 4398046511104L;
    public static final long THRESHOLD_7_BYTE = 562949953421312L;
    public static final long THRESHOLD_8_BYTE = 72057594037927936L;

    public static int encodeVLongs(long[] values, int limit, byte[] out, int into) {
        return encodeVLongs(values, 0, limit, out, into);
    }

    public static int encodedVLongsSize(long[] values, int limit) {
        return encodedVLongsSize(values, 0, limit);
    }

    public static int encodedVLongsSize(long[] values, int offset, int limit) {
        int size = 0;
        int end = offset + limit;
        for (int i = offset; i < end; ++i) {
            if (values[i] == Long.MIN_VALUE) {
                continue;
            }
            size += encodedVLongSize(values[i]);
        }
        return size;
    }

    static int encodeVLongs(long[] values, int offset, int end, byte[] out, int into) {
        for (int i = offset; i < end; ++i) {
            if (values[i] == Long.MIN_VALUE) {
                continue;
            }

            into = encodeVLong(out, values[i], into);
        }
        return into;
    }

    static long encodeVLongs(long[] values, int offset, int end, long ptr) {
        for (int i = offset; i < end; ++i) {
            if (values[i] == Long.MIN_VALUE) {
                continue;
            }

            ptr = unsafeEncodeVLong(ptr, values[i]);
        }
        return ptr;
    }

    //@formatter:off
    private static int encodeVLong(final byte[] buffer, final long val, int output) {
        if (val < THRESHOLD_1_BYTE) {
            buffer[    output] = (byte) (val       | 128L);
            return 1 + output;
        } else if (val < THRESHOLD_2_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 | 128L);
            return 2 + output;
        } else if (val < THRESHOLD_3_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 | 128L);
            return 3 + output;
        } else if (val < THRESHOLD_4_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 | 128L);
            return 4 + output;
        } else if (val < THRESHOLD_5_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 | 128L);
            return 5 + output;
        } else if (val < THRESHOLD_6_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 | 128L);
            return 6 + output;
        } else if (val < THRESHOLD_7_BYTE) {
            buffer[    output] = (byte) (val       & 127L);
            buffer[1 + output] = (byte) (val >>  7 & 127L);
            buffer[2 + output] = (byte) (val >> 14 & 127L);
            buffer[3 + output] = (byte) (val >> 21 & 127L);
            buffer[4 + output] = (byte) (val >> 28 & 127L);
            buffer[5 + output] = (byte) (val >> 35 & 127L);
            buffer[6 + output] = (byte) (val >> 42 | 128L);
            return 7 + output;
        } else if (val < THRESHOLD_8_BYTE) {
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

    //@formatter:off
    private static long unsafeEncodeVLong(long ptr, long val) {
        if (val < THRESHOLD_1_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       | 128L));
            return 1 + ptr;
        } else if (val < THRESHOLD_2_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 | 128L));
            return 2 + ptr;
        } else if (val < THRESHOLD_3_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 | 128L));
            return 3 + ptr;
        } else if (val < THRESHOLD_4_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 | 128L));
            return 4 + ptr;
        } else if (val < THRESHOLD_5_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 & 127L));
            UnsafeUtil.putByte(4 + ptr, (byte) (val >> 28 | 128L));
            return 5 + ptr;
        } else if (val < THRESHOLD_6_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 & 127L));
            UnsafeUtil.putByte(4 + ptr, (byte) (val >> 28 & 127L));
            UnsafeUtil.putByte(5 + ptr, (byte) (val >> 35 | 128L));
            return 6 + ptr;
        } else if (val < THRESHOLD_7_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 & 127L));
            UnsafeUtil.putByte(4 + ptr, (byte) (val >> 28 & 127L));
            UnsafeUtil.putByte(5 + ptr, (byte) (val >> 35 & 127L));
            UnsafeUtil.putByte(6 + ptr, (byte) (val >> 42 | 128L));
            return 7 + ptr;
        } else if (val < THRESHOLD_8_BYTE) {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 & 127L));
            UnsafeUtil.putByte(4 + ptr, (byte) (val >> 28 & 127L));
            UnsafeUtil.putByte(5 + ptr, (byte) (val >> 35 & 127L));
            UnsafeUtil.putByte(6 + ptr, (byte) (val >> 42 & 127L));
            UnsafeUtil.putByte(7 + ptr, (byte) (val >> 49 | 128L));
            return 8 + ptr;
        } else {
            UnsafeUtil.putByte(    ptr, (byte) (val       & 127L));
            UnsafeUtil.putByte(1 + ptr, (byte) (val >>  7 & 127L));
            UnsafeUtil.putByte(2 + ptr, (byte) (val >> 14 & 127L));
            UnsafeUtil.putByte(3 + ptr, (byte) (val >> 21 & 127L));
            UnsafeUtil.putByte(4 + ptr, (byte) (val >> 28 & 127L));
            UnsafeUtil.putByte(5 + ptr, (byte) (val >> 35 & 127L));
            UnsafeUtil.putByte(6 + ptr, (byte) (val >> 42 & 127L));
            UnsafeUtil.putByte(7 + ptr, (byte) (val >> 49 & 127L));
            UnsafeUtil.putByte(8 + ptr, (byte) (val >> 56 | 128L));
            return 9 + ptr;
        }
    }

    /**
     * The values are equivalent to:
     *
     * {@code BitUtil.ceilDiv(64 - Long.numberOfLeadingZeros(nodeCount - 1), 7)}
     */
    public static int encodedVLongSize(final long val) {
        if (val < THRESHOLD_1_BYTE) {
            return 1;
        } else if (val < THRESHOLD_2_BYTE) {
            return 2;
        } else if (val < THRESHOLD_3_BYTE) {
            return 3;
        } else if (val < THRESHOLD_4_BYTE) {
            return 4;
        } else if (val < THRESHOLD_5_BYTE) {
            return 5;
        } else if (val < THRESHOLD_6_BYTE) {
            return 6;
        } else if (val < THRESHOLD_7_BYTE) {
            return 7;
        } else if (val < THRESHOLD_8_BYTE) {
            return 8;
        } else {
            return 9;
        }
    }

    public static long zigZag(final long value) {
        return (value >> 63) ^ (value << 1);
    }

    private VarLongEncoding() {
        throw new UnsupportedOperationException("No instances");
    }
}
