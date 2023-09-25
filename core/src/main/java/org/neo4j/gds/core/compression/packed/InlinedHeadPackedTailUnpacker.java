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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.compress.ByteArrayBuffer;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.compression.common.VarLongDecoding;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;

final class InlinedHeadPackedTailUnpacker {

    private static final int BLOCK_SIZE = AdjacencyPacking.BLOCK_SIZE;

    // Compressed
    private final ByteArrayBuffer header;
    private long targetPtr;
    private int headerLength;

    // Decompression state
    private final long[] block;
    private final MutableLong headValue;
    private int idxInBlock;
    private int blockId;
    private long lastValue;
    private int remaining;

    InlinedHeadPackedTailUnpacker() {
        this.block = new long[BLOCK_SIZE];
        this.header = new ByteArrayBuffer();
        this.headValue = new MutableLong();
    }

    void copyFrom(InlinedHeadPackedTailUnpacker other) {
        System.arraycopy(other.block, 0, this.block, 0, BLOCK_SIZE);
        this.header.ensureCapacity(other.headerLength);
        System.arraycopy(other.header.buffer, 0, this.header.buffer, 0, other.headerLength);
        this.targetPtr = other.targetPtr;
        this.headerLength = other.headerLength;
        this.idxInBlock = other.idxInBlock;
        this.blockId = other.blockId;
        this.lastValue = other.lastValue;
        this.remaining = other.remaining;
    }

    void reset(long ptr, int degree) {
        int blocks = BitUtil.ceilDiv(degree - 1, AdjacencyPacking.BLOCK_SIZE);
        this.header.ensureCapacity(blocks);
        this.headerLength = blocks;
        // Read block information part from header
        for (int i = 0; i < blocks; i++) {
            this.header.buffer[i] = UnsafeUtil.getByte(ptr + i);
        }
        ptr += blocks;
        // Var-length decode head value following the block information.
        ptr = VarLongDecoding.unsafeDecodeVLong(ptr, this.headValue);
        // We store the head value at the end of the block array.
        // The first call to next() will read that value and the
        // subsequent call will decompress the first block.
        this.block[BLOCK_SIZE - 1] = this.headValue.longValue();
        // Set the last value to the head value for correct
        // delta decoding of the first block.
        this.lastValue = this.headValue.longValue();
        // Align target ptr to read from data block
        this.targetPtr = BitUtil.align(ptr, Long.BYTES);
        this.idxInBlock = BLOCK_SIZE - 1;
        this.blockId = 0;
        this.remaining = degree - 1;
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
            byte bits = this.header.buffer[blockId];
            int length;
            if (this.remaining < BLOCK_SIZE) {
                // last block
                this.targetPtr = AdjacencyUnpacking.loopUnpack(
                    bits,
                    this.block,
                    0,
                    this.remaining,
                    this.targetPtr
                );
                length = remaining;
                this.remaining = 0;
            } else {
                this.targetPtr = AdjacencyUnpacking.unpack(bits, this.block, 0, this.targetPtr);
                this.remaining -= BLOCK_SIZE;
                length = BLOCK_SIZE;
            }
            this.lastValue = AdjacencyCompression.deltaDecode(this.block, length, this.lastValue);
            this.blockId++;
        }

        this.idxInBlock = 0;
    }
}
