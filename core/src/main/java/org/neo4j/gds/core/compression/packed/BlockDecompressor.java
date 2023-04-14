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

import org.neo4j.gds.api.compress.ByteArrayBuffer;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

import static org.neo4j.gds.core.compression.packed.AdjacencyPacker.BYTE_ARRAY_BASE_OFFSET;

final class BlockDecompressor {

    private static final int BLOCK_SIZE = AdjacencyPacking.BLOCK_SIZE;

    // Compressed
    private final ByteArrayBuffer header;
    private long targetPtr;
    private int headerLength;

    // Decompression state
    private final long[] block;

    private int idxInBlock;
    private int blockId;
    private long lastValue;

    BlockDecompressor() {
        this.block = new long[BLOCK_SIZE];
        this.header = new ByteArrayBuffer();
    }

    void reset(long ptr, int degree) {
        int headerSize = BitUtil.ceilDiv(degree, AdjacencyPacking.BLOCK_SIZE);
        long alignedHeaderSize = BitUtil.align(headerSize, Long.BYTES);

        // Read header bytes
        this.headerLength = headerSize;
        this.header.ensureCapacity(headerSize);
        UnsafeUtil.copyMemory(null, ptr, this.header.buffer, BYTE_ARRAY_BASE_OFFSET, headerSize);

        this.targetPtr = ptr + alignedHeaderSize;
        this.idxInBlock = 0;
        this.blockId = 0;
        this.lastValue = 0;

        this.decompressBlock();
    }

    long next() {
        if (this.idxInBlock == BLOCK_SIZE) {
            decompressBlock();
        }
        return block[this.idxInBlock++];
    }

    long peek() {
        if (this.idxInBlock == BLOCK_SIZE) {
            decompressBlock();
        }
        return block[this.idxInBlock];
    }

    long advanceBy(int steps) {
        // Due to delta encoded target ids, we can't yet skip blocks
        // as we need to decompress all the previous blocks to get
        // the correct target id.
        while (this.idxInBlock + steps >= BLOCK_SIZE) {
            steps = this.idxInBlock + steps - BLOCK_SIZE;
            decompressBlock();
        }
        this.idxInBlock += steps;
        return block[this.idxInBlock++];
    }

    private void decompressBlock() {
        if (this.blockId < this.headerLength) {
            // block unpacking
            byte blockHeader = this.header.buffer[blockId];
            this.targetPtr = AdjacencyUnpacking.unpack(blockHeader, this.block, 0, this.targetPtr);
            long value = this.lastValue;
            for (int i = 0; i < AdjacencyPacking.BLOCK_SIZE; i++) {
                value = this.block[i] += value;
            }
            this.lastValue = value;
            this.blockId++;
        }

        this.idxInBlock = 0;
    }
}
