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

import org.neo4j.gds.core.compression.common.AdjacencyCompression;

final class BlockDecompressor {

    private static final int BLOCK_SIZE = AdjacencyPacking.BLOCK_SIZE;

    private final boolean isDeltaCompressed;

    // Compressed
    private long ptr;
    private byte[] header;
    private int length;

    // Decompression state
    private final long[] block;

    private int idxInBlock;
    private int blockId;
    private int blockOffset;
    private long lastValue;


    BlockDecompressor(int flags) {
        this.isDeltaCompressed = (flags & AdjacencyPacker.DELTA) == AdjacencyPacker.DELTA;
        this.block = new long[BLOCK_SIZE];
    }

    void reset(Compressed compressed) {
        this.ptr = compressed.address();
        this.header = compressed.header();
        this.length = compressed.length();
        this.idxInBlock = 0;
        this.blockId = 0;
        this.blockOffset = 0;
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

    private void decompressBlock() {
        if (this.blockId < this.header.length) {
            // block unpacking
            byte blockHeader = this.header[blockId];
            this.ptr = AdjacencyUnpacking.unpack(blockHeader, this.block, 0, this.ptr);
            if (this.isDeltaCompressed) {
                long value = this.lastValue;
                for (int i = 0; i < AdjacencyPacking.BLOCK_SIZE; i++) {
                    value = this.block[i] += value;
                }
                this.lastValue = value;
            }
            this.blockOffset += BLOCK_SIZE;
            this.blockId++;
        } else {
            // tail decompression
            int tailLength = this.length - this.blockOffset;
            if (this.isDeltaCompressed) {
                AdjacencyCompression.decompressAndPrefixSum(tailLength, this.lastValue, this.ptr, this.block, 0);
            } else {
                AdjacencyCompression.decompress(tailLength, this.ptr, this.block, 0);
            }
        }

        this.idxInBlock = 0;
    }
}
