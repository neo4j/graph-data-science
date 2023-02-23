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
package org.neo4j.gds.core.compression.varlong;

import org.neo4j.gds.core.loading.MutableIntValue;

import java.util.Arrays;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;
import static org.neo4j.gds.core.compression.common.VarLongDecoding.decodeDeltaVLongs;

final class AdjacencyDecompressingReader {

    static final int CHUNK_SIZE = 64;

    private final long[] block;
    private int pos;
    private byte[] array;
    private int offset;

    AdjacencyDecompressingReader() {
        this.block = new long[CHUNK_SIZE];
    }

    //@formatter:off
    static long readLong(byte[] array, int offset) {
        return   array[    offset] & 255L        |
                (array[1 + offset] & 255L) <<  8 |
                (array[2 + offset] & 255L) << 16 |
                (array[3 + offset] & 255L) << 24 |
                (array[4 + offset] & 255L) << 32 |
                (array[5 + offset] & 255L) << 40 |
                (array[6 + offset] & 255L) << 48 |
                (array[7 + offset] & 255L) << 56;
    }
    //@formatter:on

    void copyFrom(AdjacencyDecompressingReader other) {
        System.arraycopy(other.block, 0, block, 0, CHUNK_SIZE);
        pos = other.pos;
        array = other.array;
        offset = other.offset;
    }

    int reset(byte[] adjacencyPage, int offset, int degree) {
        this.array = adjacencyPage;
        this.offset = decodeDeltaVLongs(0L, adjacencyPage, offset, Math.min(degree, CHUNK_SIZE), block);
        pos = 0;
        return degree;
    }

    long next(int remaining) {
        int pos = this.pos++;
        if (pos < CHUNK_SIZE) {
            return block[pos];
        }
        // We need to set this to 1 since the target
        // at index 0 is returned from readNextBlock.
        this.pos = 1;
        return readNextBlock(remaining);
    }

    long peek(int remaining) {
        int pos = this.pos;
        if (pos < CHUNK_SIZE) {
            return block[pos];
        }
        // We need to set this to 0 since the target
        // at index 0 is returned from readNextBlock
        // and we don't want to advance the cursor.
        this.pos = 0;
        return readNextBlock(remaining);
    }

    private long readNextBlock(int remaining) {
        offset = decodeDeltaVLongs(block[CHUNK_SIZE - 1], array, offset, Math.min(remaining, CHUNK_SIZE), block);
        return block[0];
    }

    long skipUntil(long target, int remaining, MutableIntValue consumed) {
        int pos = this.pos;
        long[] block = this.block;
        int available = remaining;

        // skip blocks until we have either not enough available to decode or have advanced far enough
        while (available > CHUNK_SIZE - pos && block[CHUNK_SIZE - 1] <= target) {
            int skippedInThisBlock = CHUNK_SIZE - pos;
            available -= skippedInThisBlock;
            int needToDecode = Math.min(CHUNK_SIZE, available);
            offset = decodeDeltaVLongs(block[CHUNK_SIZE - 1], array, offset, needToDecode, block);
            pos = 0;
        }

        // last block
        if(available <= 0) {
            return NOT_FOUND;
        }

        int targetPos = findPosStrictlyGreaterInBlock(target, pos, Math.min(pos + available, CHUNK_SIZE), block);

        if (targetPos == NOT_FOUND) {
            // We exhausted the cursor and did not find the target.
            consumed.value = remaining;
            this.pos = pos + available;

            return NOT_FOUND;
        }

        // we need to consume including targetPos, not to it, therefore +1
        available -= (1 + targetPos - pos);
        consumed.value = remaining - available;
        this.pos = 1 + targetPos;
        return block[targetPos];
    }

    long advance(long target, int remaining, MutableIntValue consumed) {
        int pos = this.pos;
        long[] block = this.block;
        int available = remaining;

        // skip blocks until we have either not enough available to decode or have advanced far enough
        while (available > CHUNK_SIZE - pos && block[CHUNK_SIZE - 1] < target) {
            int skippedInThisBlock = CHUNK_SIZE - pos;
            available -= skippedInThisBlock;
            int needToDecode = Math.min(CHUNK_SIZE, available);
            offset = decodeDeltaVLongs(block[CHUNK_SIZE - 1], array, offset, needToDecode, block);
            pos = 0;
        }

        // last block
        int targetPos = findPosInBlock(target, pos, Math.min(pos + available, CHUNK_SIZE), block);

        if (targetPos == NOT_FOUND) {
            // We exhausted the cursor and did not find the target.
            consumed.value = remaining;
            this.pos = pos + available;

            return NOT_FOUND;
        }

        // we need to consume including targetPos, not to it, therefore +1
        available -= (1 + targetPos - pos);
        consumed.value = remaining - available;
        this.pos = 1 + targetPos;
        return block[targetPos];
    }

    long advanceBy(int skip, int remaining, MutableIntValue consumed) {
        assert skip < remaining : "skip must be less than remaining but got skip=" + skip + " remaining=" + remaining;

        int availableBeyondSkip = remaining - skip;
        int initialSkip = skip;
        int pos = this.pos;
        long[] block = this.block;

        // skip blocks until we have either not enough available to decode or have advanced far enough
        while (skip >= CHUNK_SIZE - pos) {
            int skippedInThisBlock = CHUNK_SIZE - pos;
            skip -= skippedInThisBlock;
            // we need to decode the full block of the adjacency list, even if we would only
            // skip it partially. We would get wrong data after the skip position otherwise.
            int needToDecode = Math.min(CHUNK_SIZE, skip + availableBeyondSkip);
            offset = decodeDeltaVLongs(block[CHUNK_SIZE - 1], array, offset, needToDecode, block);
            pos = 0;
        }

        // last block
        int targetPos = pos + skip;
        // we need to consume including targetPos, not to it, therefore +1
        skip -= (1 + targetPos - pos);
        this.pos = 1 + targetPos;
        // this should be the initialSkip + 1
        consumed.value = remaining - availableBeyondSkip - skip;
        assert consumed.value == initialSkip + 1 : "Meant to skip " + initialSkip + " targets but only " + consumed.value + " were skipped";

        return block[targetPos];
    }

    private int findPosStrictlyGreaterInBlock(long target, int pos, int limit, long[] block) {
        return findPosInBlock(1L + target, pos, limit, block);
    }

    private int findPosInBlock(long target, int pos, int limit, long[] block) {
        int targetPos = Arrays.binarySearch(block, pos, limit, target);
        if (targetPos < 0) {
            if (-targetPos > limit) {
                return (int) NOT_FOUND;
            }
            targetPos = Math.min(-1 - targetPos, limit - 1);
        }
        return targetPos;
    }
}
