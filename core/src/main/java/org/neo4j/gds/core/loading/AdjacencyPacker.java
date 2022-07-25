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

import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.Arrays;

public final class AdjacencyPacker {

    private AdjacencyPacker() {}

    public static final class Compressed {
        private final long address;
        private final long bytes;
        private final byte[] bits;

        public Compressed(long address, long bytes, byte[] bits) {
            this.address = address;
            this.bytes = bytes;
            this.bits = bits;
        }

        public long bytesUsed() {
            return bytes + bits.length;
        }

        public void free() {
            UnsafeUtil.free(address, bytes, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        public String toString() {
            return "Compressed{address=" + address + ", bytes=" + bytes + ", blocks=" + bits.length + '}';
        }
    }

    public static final int PASS = 0;
    public static final int SORT = 1 << 22;
    public static final int DELTA = 1 << 23;

    private static final int MASK = ~(SORT | DELTA);

    public static Compressed compress(long[] values, int offset, int length, int flags) {
        if ((flags & SORT) == SORT) {
            Arrays.sort(values, offset, offset + length);
        }
        if ((flags & DELTA) == DELTA) {
            var ordinal = flags & MASK;
            var aggregation = Aggregation.resolve(Aggregation.values()[ordinal]);
            length = AdjacencyCompression.deltaEncodeSortedValues(values, offset, length, aggregation);
        }
        return compress(values, offset, length);
    }

    public static Compressed compress(long[] values, int offset, int length) {
        int end = offset + length;

        int blocks = length / AdjacencyPacking.BLOCK_SIZE;
        var allBits = new byte[blocks];

        long bytes = 0L;
        int i = offset;
        int blockIdx = 0;
        for (; i + AdjacencyPacking.BLOCK_SIZE <= end; i += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, i, AdjacencyPacking.BLOCK_SIZE);
            bytes += bytesNeeded(bits);
            allBits[blockIdx++] = (byte) bits;
        }

        bytes = BitUtil.align(bytes, Long.BYTES);
        long mem = UnsafeUtil.allocateMemory(bytes, EmptyMemoryTracker.INSTANCE);
        long ptr = mem;

        i = offset;
        for (byte bits : allBits) {
            ptr = AdjacencyPacking.pack(bits, values, i, ptr);
            i += AdjacencyPacking.BLOCK_SIZE;
        }

        return new Compressed(mem, bytes, allBits);
    }

    public static long[] decompressAndPrefixSum(Compressed compressed) {
        long[] values = new long[compressed.bits.length * AdjacencyPacking.BLOCK_SIZE];

        long value = values[0];
        int offset = 0;
        long ptr = compressed.address;
        for (byte bits : compressed.bits) {
            ptr = AdjacencyPacking.unpack(bits, values, offset, ptr);
            for (int i = 0; i < AdjacencyPacking.BLOCK_SIZE; i++) {
                value = values[offset + i] += value;
            }
            offset += AdjacencyPacking.BLOCK_SIZE;
        }


        return values;
    }

    public static long[] decompress(Compressed compressed) {
        long[] values = new long[compressed.bits.length * AdjacencyPacking.BLOCK_SIZE];

        int offset = 0;
        long ptr = compressed.address;
        for (byte bits : compressed.bits) {
            ptr = AdjacencyPacking.unpack(bits, values, offset, ptr);
            offset += AdjacencyPacking.BLOCK_SIZE;
        }

        return values;
    }

    private static int bitsNeeded(long[] values, int offset, int length) {
        long bits = 0L;
        for (int i = offset; i < offset + length; i++) {
            bits |= values[i];
        }
        return Long.SIZE - Long.numberOfLeadingZeros(bits);
    }

    private static int bytesNeeded(int bits) {
        return BitUtil.ceilDiv(AdjacencyPacking.BLOCK_SIZE * bits, Byte.SIZE);
    }
}
