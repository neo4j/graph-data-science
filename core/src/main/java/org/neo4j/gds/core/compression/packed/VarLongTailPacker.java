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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

import java.util.Arrays;

import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodedVLongsSize;
import static org.neo4j.gds.core.compression.packed.AdjacencyPacker.BYTE_ARRAY_BASE_OFFSET;
import static org.neo4j.gds.core.compression.packed.AdjacencyPacker.bitsNeeded;
import static org.neo4j.gds.core.compression.packed.AdjacencyPacker.bytesNeeded;

/**
 * Compresses values in blocks of {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} using bit-packing.
 * <p>
 * If a block to compress has less than {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} values,
 * this strategy uses var-long encoding to compress the values in that block.
 */
final class VarLongTailPacker {

    static long compress(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        int length,
        Aggregation aggregation,
        MutableInt degree,
        MemoryTracker memoryTracker
    ) {
        Arrays.sort(values, 0, length);
        return deltaCompress(allocator, slice, values, length, aggregation, degree, memoryTracker);
    }

    static long compressWithProperties(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        int length,
        MemoryTracker memoryTracker
    ) {
        return preparePacking(allocator, slice, values, length, memoryTracker);
    }

    private static long deltaCompress(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        int length,
        Aggregation aggregation,
        MutableInt degree,
        MemoryTracker memoryTracker
    ) {
        if (length > 0) {
            length = AdjacencyCompression.deltaEncodeSortedValues(values, 0, length, aggregation);
        }

        degree.setValue(length);

        return preparePacking(allocator, slice, values, length, memoryTracker);
    }

    private static long preparePacking(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        int length,
        MemoryTracker memoryTracker
    ) {
        int blocks = length / AdjacencyPacking.BLOCK_SIZE;
        var header = new byte[blocks];

        long blockBytes = 0L;
        int offset = 0;
        int blockIdx = 0;

        for (; blockIdx < blocks; blockIdx++, offset += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, offset, AdjacencyPacking.BLOCK_SIZE);
            memoryTracker.recordHeaderBits(bits);
            blockBytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }

        int tailLength = length - offset;
        long tailBytes = encodedVLongsSize(values, length - tailLength, tailLength);

        return runPacking(
            allocator,
            slice,
            values,
            header,
            blockBytes,
            tailBytes,
            length,
            tailLength,
            memoryTracker
        );
    }

    private static long runPacking(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        byte[] header,
        long blockBytes,
        long tailBytes,
        int length,
        int tailLength,
        MemoryTracker memoryTracker
    ) {
        assert values.length % AdjacencyPacking.BLOCK_SIZE == 0 : "values length must be a multiple of " + AdjacencyPacking.BLOCK_SIZE + ", but was " + values.length;

        long headerSize = header.length * Byte.BYTES;
        // we must add padding between the header and the data bytes
        // to avoid writing unaligned longs
        long alignedHeaderSize = BitUtil.align(headerSize, Long.BYTES);
        long fullSize = alignedHeaderSize + blockBytes + tailBytes;
        // we must align to long because we write in terms of longs, not single bytes
        long alignedFullSize = BitUtil.align(fullSize, Long.BYTES);
        int allocationSize = Math.toIntExact(alignedFullSize);

        long adjacencyOffset = allocator.allocate(allocationSize, slice);

        Address address = slice.slice();
        long ptr = address.address() + slice.offset();

        // write header
        UnsafeUtil.copyMemory(header, BYTE_ARRAY_BASE_OFFSET, null, ptr, headerSize);
        ptr += alignedHeaderSize;

        // main packing loop
        int in = 0;
        for (byte bits : header) {
            memoryTracker.recordBlockStatistics(values, in, AdjacencyPacking.BLOCK_SIZE);
            ptr = AdjacencyPacking.pack(bits, values, in, ptr);
            in += AdjacencyPacking.BLOCK_SIZE;
        }

        AdjacencyCompression.compress(values, length - tailLength, tailLength, ptr);

        return adjacencyOffset;
    }

    private VarLongTailPacker() {}
}
