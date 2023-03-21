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

import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

import java.util.Arrays;

public final class AdjacencyPacker2 {

    private AdjacencyPacker2() {}

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

    public static long compress(
        AdjacencyListBuilder.Allocator<Long> allocator,
        AdjacencyListBuilder.Slice<Long> slice,
        long[] values,
        int length,
        Aggregation aggregation
    ) {
        Arrays.sort(values, 0, length);
        return deltaCompress(allocator, slice, values, length, aggregation);
    }

    public static long compressWithProperties(
        AdjacencyListBuilder.Allocator<Long> allocator,
        AdjacencyListBuilder.Slice<Long> slice,
        long[] values,
        long[][] properties,
        int length,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
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

        return preparePacking(allocator, slice, values, length);
    }

    private static long deltaCompress(
        AdjacencyListBuilder.Allocator<Long> allocator,
        AdjacencyListBuilder.Slice<Long> slice,
        long[] values,
        int length,
        Aggregation aggregation
    ) {
        if (length > 0) {
            length = AdjacencyCompression.deltaEncodeSortedValues(values, 0, length, aggregation);
        }
        return preparePacking(allocator, slice, values, length);
    }

    private static long preparePacking(
        AdjacencyListBuilder.Allocator<Long> allocator,
        AdjacencyListBuilder.Slice<Long> slice,
        long[] values,
        int length
    ) {
        int blocks = BitUtil.ceilDiv(length, AdjacencyPacking.BLOCK_SIZE);
        var header = new byte[blocks];

        long bytes = 0L;
        int offset = 0;
        int blockIdx = 0;

        for (; blockIdx < blocks - 1; blockIdx++, offset += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, offset, AdjacencyPacking.BLOCK_SIZE);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }
        // "tail" block, may be smaller than BLOCK_SIZE
        {
            int bits = bitsNeeded(values, offset, length - offset);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }

        return runPacking(allocator, slice, values, length, header, bytes);
    }

    public static final int BYTE_ARRAY_BASE_OFFSET = UnsafeUtil.arrayBaseOffset(byte[].class);
    private static final int BYTE_ARRAY_INDEX_SCALE = UnsafeUtil.arrayIndexScale(byte[].class);

    private static long runPacking(
        AdjacencyListBuilder.Allocator<Long> allocator,
        AdjacencyListBuilder.Slice<Long> slice,
        long[] values,
        int length,
        byte[] header,
        long requiredBytes
    ) {
        assert values.length % AdjacencyPacking.BLOCK_SIZE == 0 : "values length must be a multiple of " + AdjacencyPacking.BLOCK_SIZE + ", but was " + values.length;

        long headerSize = header.length * Byte.BYTES;
        // we must add padding between the header and the data bytes
        // to avoid writing unaligned longs
        long alignedHeaderSize = BitUtil.align(headerSize, Long.BYTES);
        long fullSize = alignedHeaderSize + requiredBytes;
        // we must align to long because we write in terms of longs, not single bytes
        long alignedFullSize = BitUtil.align(fullSize, Long.BYTES);
        int allocationSize = Math.toIntExact(alignedFullSize);

        long adjacencyOffset = allocator.allocate(allocationSize, slice);

        long mem = slice.slice();
        long ptr = mem + slice.offset();

        // write header
        UnsafeUtil.copyMemory(header, BYTE_ARRAY_BASE_OFFSET, null, ptr, headerSize);
        ptr += alignedHeaderSize;

        // main packing loop
        int in = 0;
        for (byte bits : header) {
            ptr = AdjacencyPacking.pack(bits, values, in, ptr);
            in += AdjacencyPacking.BLOCK_SIZE;
        }

        // TODO: do we need to return alignedBytes somehow?
        ((ModifiableSlice<?>) slice).setLength(length);
        return adjacencyOffset;
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
