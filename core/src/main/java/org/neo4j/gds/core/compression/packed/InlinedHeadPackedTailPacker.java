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
import org.neo4j.gds.core.compression.common.VarLongEncoding;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

import java.util.Arrays;

import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.bitsNeeded;
import static org.neo4j.gds.core.compression.packed.AdjacencyPackerUtil.bytesNeeded;

/**
 * Compresses values in blocks of {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} using bit-packing.
 * <p>
 * This strategy compressed the first (usually largest) value separately from the rest of the values and leaves a 0-value in
 * at the first index. It uses var-length encoding and writes the encoded value after the header, preceding the data.
 * <p>
 * If a block to compress has less than {@link org.neo4j.gds.core.compression.packed.AdjacencyPacking#BLOCK_SIZE} values,
 * this strategy uses a loop-based packing approach to compress the values in that block.
 */
final class InlinedHeadPackedTailPacker {

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
        // Number of blocks we need to pack all values.
        int blockCount;
        byte[] header;
        int offset;

        if (length > 0) {
            // We assume that the first value is the largest exception within the adjacency list.
            // We move it out of the values and compress it separately as part of the header byte
            // array using var-length encoding.
            blockCount = BitUtil.ceilDiv(length - 1, AdjacencyPacking.BLOCK_SIZE);
            int headSize = VarLongEncoding.encodedVLongSize(values[0]);
            header = new byte[blockCount + headSize];
            VarLongEncoding.encodeVLong(header, values[0], blockCount);
            offset = 1; // skip the first value
        } else {
            blockCount = BitUtil.ceilDiv(length, AdjacencyPacking.BLOCK_SIZE);
            header = new byte[blockCount];
            offset = 0;
        }

        boolean hasTail = (length - offset) == 0 || (length - offset) % AdjacencyPacking.BLOCK_SIZE != 0;
        long bytes = 0L;
        int blockIdx = 0;
        int lastFullBlock = hasTail ? blockCount - 1 : blockCount;

        for (; blockIdx < lastFullBlock; blockIdx++, offset += AdjacencyPacking.BLOCK_SIZE) {
            int bits = bitsNeeded(values, offset, AdjacencyPacking.BLOCK_SIZE);
            memoryTracker.recordHeaderBits(bits);
            bytes += bytesNeeded(bits);
            header[blockIdx] = (byte) bits;
        }
        // "tail" block, may be smaller than BLOCK_SIZE
        if (hasTail) {
            int bits = bitsNeeded(values, offset, length - offset);
            memoryTracker.recordHeaderBits(bits);
            bytes += bytesNeeded(bits, length - offset);
            header[blockIdx] = (byte) bits;
        }

        return runPacking(
            allocator,
            slice,
            values,
            header,
            bytes,
            blockCount,
            (length - offset),
            memoryTracker
        );
    }

    private static long runPacking(
        AdjacencyListBuilder.Allocator<Address> allocator,
        AdjacencyListBuilder.Slice<Address> slice,
        long[] values,
        byte[] header,
        long bytes,
        int blockCount,
        int tailLength,
        MemoryTracker memoryTracker
    ) {
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
        // increment ptr
        ptr += alignedHeaderSize;

        // main packing loop
        boolean hasTail = tailLength > 0;
        // We always skip the first element, because it's stored in the header
        int in = 1;
        int fullBlocks = hasTail ? blockCount - 1 : blockCount;

        // main packing loop
        for (int i = 0; i < fullBlocks; i++) {
            byte bits = header[i];
            memoryTracker.recordBlockStatistics(values, in, AdjacencyPacking.BLOCK_SIZE);
            ptr = AdjacencyPacking.pack(bits, values, in, ptr);
            in += AdjacencyPacking.BLOCK_SIZE;
        }

        // tail packing
        if (hasTail) {
            byte bits = header[blockCount - 1];
            memoryTracker.recordBlockStatistics(values, in, tailLength);
            ptr = AdjacencyPacking.loopPack(bits, values, in, tailLength, ptr);
        }

        if (ptr > initialPtr + allocationSize)
            throw new AssertionError("Written more bytes than allocated. ptr=" + ptr + ", initialPtr=" + initialPtr + ", allocationSize=" + allocationSize);


        return adjacencyOffset;
    }

    private InlinedHeadPackedTailPacker() {}
}
