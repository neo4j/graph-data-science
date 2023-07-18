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

import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.bitsNeeded;
import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.bytesNeeded;

/**
 * Compresses values in blocks of {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} using bit-packing.
 * <p>
 * If a block to compress has less than {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} values,
 * this strategy uses a loop-based packing approach to compress the values in that block.
 */
final class PackedTailPacker {

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

        if (length > 0) {
            length = AdjacencyCompression.deltaEncodeSortedValues(values, 0, length, aggregation);
        }

        degree.setValue(length);

        return preparePacking(allocator, slice, values, length, memoryTracker);
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

    private static long preparePacking(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        int length,
        MemoryTracker memoryTracker
    ) {
        boolean hasTail = length == 0 || length % AdjacencyPacking.BLOCK_SIZE != 0;
        int blocks = BitUtil.ceilDiv(length, AdjacencyPacking.BLOCK_SIZE);
        var header = new byte[blocks];

        long bytes = 0L;
        int offset = 0;
        int blockIdx = 0;
        int lastFullBlock = hasTail ? blocks - 1 : blocks;

        for (; blockIdx < lastFullBlock; blockIdx++, offset += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, offset, AdjacencyPacking.BLOCK_SIZE);
            memoryTracker.recordHeaderBits(bits);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }
        // "tail" block, may be smaller than BLOCK_SIZE
        int tailLength = (length - offset);
        if (hasTail) {
            int bits = bitsNeeded(values, offset, tailLength);
            memoryTracker.recordHeaderBits(bits);
            bytes += bytesNeeded(bits, tailLength);
            header[blockIdx] = (byte) bits;
        }

        return runPacking(
            allocator,
            slice,
            values,
            header,
            bytes,
            tailLength,
            memoryTracker
        );
    }

    private static long runPacking(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        byte[] header,
        long bytes,
        int tailLength,
        MemoryTracker memoryTracker
    ) {
        assert values.length % AdjacencyPacking.BLOCK_SIZE == 0 : "values length must be a multiple of " + AdjacencyPacking.BLOCK_SIZE + ", but was " + values.length;

        long headerSize = header.length * Byte.BYTES;
        // we must add padding between the header and the data bytes
        // to avoid writing unaligned longs
        long alignedHeaderSize = BitUtil.align(headerSize, Long.BYTES);
        long fullSize = alignedHeaderSize + bytes;
        // we must align to long because we write in terms of longs, not single bytes
        long alignedFullSize = BitUtil.align(fullSize, Long.BYTES);
        int allocationSize = Math.toIntExact(alignedFullSize);
        memoryTracker.recordHeaderAllocation(alignedHeaderSize);

        long adjacencyOffset = allocator.allocate(allocationSize, slice);

        Address address = slice.slice();
        long ptr = address.address() + slice.offset();
        long initialPtr = ptr;

        // write header
        UnsafeUtil.copyMemory(header, BYTE_ARRAY_BASE_OFFSET, null, ptr, headerSize);
        ptr += alignedHeaderSize;

        // main packing loop
        boolean hasTail = tailLength > 0;
        int in = 0;
        int headerLength = hasTail ? header.length - 1 : header.length;

        for (int i = 0; i < headerLength; i++) {
            byte bits = header[i];
            memoryTracker.recordBlockStatistics(values, in, AdjacencyPacking.BLOCK_SIZE);
            ptr = AdjacencyPacking.pack(bits, values, in, ptr);
            in += AdjacencyPacking.BLOCK_SIZE;
        }

        // tail packing
        if (hasTail) {
            byte bits = header[header.length - 1];
            memoryTracker.recordBlockStatistics(values, in, tailLength);
            ptr = AdjacencyPacking.loopPack(bits, values, in, tailLength, ptr);
        }

        if (ptr > initialPtr + allocationSize)
            throw new AssertionError("Written more bytes than allocated. ptr=" + ptr + ", initialPtr=" + initialPtr + ", allocationSize=" + allocationSize);


        return adjacencyOffset;
    }

    private PackedTailPacker() {}
}
