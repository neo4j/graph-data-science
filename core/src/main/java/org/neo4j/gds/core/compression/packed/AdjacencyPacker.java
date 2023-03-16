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
package org.neo4j.gds.core.compression.packed;

import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.Arrays;

public final class AdjacencyPacker {

    private AdjacencyPacker() {}

    public static long align(long length) {
        return BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE);
    }

    public static int align(int length) {
        return (int) BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE);
    }

    private static final Aggregation[] AGGREGATIONS = Aggregation.values();

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
            var aggregation = Aggregation.resolve(AGGREGATIONS[ordinal]);
            return deltaCompress(values, offset, length, aggregation);
        } else {
            return compress(values, offset, length);
        }
    }

    public static Compressed compressWithProperties(long[] values, long[][] properties, int length, Aggregation[] aggregations, boolean noAggregation) {
        // TODO: works only for sorted + delta compression; do we need more?
        if (length > 0) {
            // sort, delta encode, reorder and aggregate properties
            length = AdjacencyCompression.applyDeltaEncoding(
                values,
                length,
                properties,
                aggregations,
                noAggregation
            );
        }
        // pack targets
        var compressed = preparePacking(values, 0, length);
        // link properties
        compressed.properties(properties);
        return compressed;
    }

    public static Compressed compress(long[] values, int offset, int length) {
        return preparePacking(values, offset, length);
    }

    private static Compressed deltaCompress(long[] values, int offset, int length, Aggregation aggregation) {
        if (length > 0) {
            length = AdjacencyCompression.deltaEncodeSortedValues(values, offset, length, aggregation);
        }
        return preparePacking(values, offset, length);
    }

    private static Compressed preparePacking(long[] values, int start, int length) {
        int end = start + length;

        int blocks = BitUtil.ceilDiv(length, AdjacencyPacking.BLOCK_SIZE);
        var header = new byte[blocks];

        long bytes = 0L;
        int offset = start;
        int blockIdx = 0;

        for (; blockIdx < blocks - 1; blockIdx++, offset += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, offset, AdjacencyPacking.BLOCK_SIZE);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }
        // "tail" block, may be smaller than BLOCK_SIZE
        {
            int bits = bitsNeeded(values, offset, end - offset);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }

        return runPacking(values, start, length, header, bytes);
    }

    private static Compressed runPacking(
        long[] values,
        int offset,
        int length,
        byte[] header,
        long requiredBytes
    ) {
        assert values.length % AdjacencyPacking.BLOCK_SIZE == 0 : "values length must be a multiple of " + AdjacencyPacking.BLOCK_SIZE + ", but was " + values.length;

        // we must align to long because we write in terms of longs, not single bytes
        var alignedBytes = BitUtil.align(requiredBytes, Long.BYTES);

        long mem = UnsafeUtil.allocateMemory(alignedBytes, EmptyMemoryTracker.INSTANCE);
        long ptr = mem;

        // main packing loop
        int in = offset;
        for (byte bits : header) {
            ptr = AdjacencyPacking.pack(bits, values, in, ptr);
            in += AdjacencyPacking.BLOCK_SIZE;
        }

        return new Compressed(mem, alignedBytes, header, length);
    }

    public static long[] decompressAndPrefixSum(Compressed compressed) {
        long ptr = compressed.address();
        byte[] header = compressed.header();
        long[] values = new long[(int) BitUtil.align(compressed.length(), AdjacencyPacking.BLOCK_SIZE)];

        // main unpacking loop
        int offset = 0;
        long value = 0L;
        for (byte bits : header) {
            ptr = AdjacencyUnpacking.unpack(bits, values, offset, ptr);
            for (int i = 0; i < AdjacencyPacking.BLOCK_SIZE; i++) {
                value = values[offset + i] += value;
            }
            offset += AdjacencyPacking.BLOCK_SIZE;
        }

        return Arrays.copyOf(values, compressed.length());
    }

    public static long[] decompress(Compressed compressed) {
        long ptr = compressed.address();
        byte[] header = compressed.header();
        long[] values = new long[(int) BitUtil.align(compressed.length(), AdjacencyPacking.BLOCK_SIZE)];

        // main unpacking loop
        int offset = 0;
        for (byte bits : header) {
            ptr = AdjacencyUnpacking.unpack(bits, values, offset, ptr);
            offset += AdjacencyPacking.BLOCK_SIZE;
        }

        return Arrays.copyOf(values, compressed.length());
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
