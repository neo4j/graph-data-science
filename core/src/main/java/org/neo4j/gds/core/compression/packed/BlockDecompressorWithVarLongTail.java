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

import static org.neo4j.gds.core.compression.common.VarLongDecoding.unsafeDecodeDeltaVLongs;
import static org.neo4j.gds.core.compression.packed.AdjacencyPacker.BYTE_ARRAY_BASE_OFFSET;

final class BlockDecompressorWithVarLongTail {

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
    private int remaining;

    BlockDecompressorWithVarLongTail() {
        this.block = new long[BLOCK_SIZE];
        this.header = new ByteArrayBuffer();
    }

    void copyFrom(BlockDecompressorWithVarLongTail other) {
        System.arraycopy(other.block, 0, this.block, 0, BLOCK_SIZE);
        System.arraycopy(other.header.buffer, 0, this.header.buffer, 0, other.header.length);
        this.targetPtr = other.targetPtr;
        this.headerLength = other.headerLength;
        this.idxInBlock = other.idxInBlock;
        this.blockId = other.blockId;
        this.lastValue = other.lastValue;
        this.remaining = other.remaining;
    }

    void reset(long ptr, int degree) {
        int headerSize = degree / AdjacencyPacking.BLOCK_SIZE;
        long alignedHeaderSize = BitUtil.align(headerSize, Long.BYTES);

        // Read header bytes
        this.headerLength = headerSize;
        this.header.ensureCapacity(headerSize);
        UnsafeUtil.copyMemory(null, ptr, this.header.buffer, BYTE_ARRAY_BASE_OFFSET, headerSize);

        this.targetPtr = ptr + alignedHeaderSize;
        this.idxInBlock = 0;
        this.blockId = 0;
        this.lastValue = 0;
        this.remaining = degree;

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
        } else {
            long ptr = unsafeDecodeDeltaVLongs(
                this.remaining,
                this.lastValue,
                this.targetPtr,
                this.block,
                0
            );
            this.targetPtr = 0;
        }

        this.remaining -= BLOCK_SIZE;
        this.idxInBlock = 0;
    }
}
