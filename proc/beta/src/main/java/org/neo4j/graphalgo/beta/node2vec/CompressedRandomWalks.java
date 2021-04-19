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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.graphalgo.core.loading.ZigZagLongDecoding;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.Iterator;

import static org.neo4j.graphalgo.core.loading.VarLongEncoding.encodeVLongs;
import static org.neo4j.graphalgo.core.loading.VarLongEncoding.encodedVLongSize;
import static org.neo4j.graphalgo.core.loading.VarLongEncoding.zigZag;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class CompressedRandomWalks {
    private final HugeObjectArray<byte[]> compressedWalks;
    private final HugeLongArray walkLengths;

    private int maxWalkLength;
    private long size = 0L;

    public CompressedRandomWalks(long maxWalkCount, AllocationTracker tracker) {
        this.compressedWalks = HugeObjectArray.newArray(byte[].class, maxWalkCount, tracker);
        this.walkLengths = HugeLongArray.newArray(maxWalkCount, tracker);
    }

    public void add(long... walk) {
        long currentLastValue = 0L;
        int requiredBytes = 0;

        for (int i = 0; i < walk.length; i++) {
            var delta = walk[i] - currentLastValue;
            var compressedValue = zigZag(delta);
            currentLastValue = walk[i];
            walk[i] = compressedValue;
            requiredBytes += encodedVLongSize(compressedValue);
        }

        var compressedData = new byte[requiredBytes];
        encodeVLongs(walk, walk.length, compressedData, 0);

        var currentIndex = size++;
        compressedWalks.set(currentIndex, compressedData);
        walkLengths.set(currentIndex, walk.length);
        if (walk.length > maxWalkLength) {
            maxWalkLength = walk.length;
        }
    }

    public Iterator<long[]> iterator(long startIndex, long length) {
        var endIndex = startIndex + length - 1;
        if (startIndex >= size() || endIndex >= size()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Requested iterator chunk exceeds the number of stored random walks. Requested %d-%d, actual size %d",
                    startIndex,
                    endIndex,
                    size()
                )
            );
        }

        return new CompressedWalkIterator(startIndex, endIndex, compressedWalks, walkLengths, maxWalkLength);
    }

    public long size() {
        return size;
    }

    public int walkLength(long index) {
        return (int) walkLengths.get(index);
    }

    public static class CompressedWalkIterator implements Iterator<long[]> {
        private final HugeCursor<byte[][]> cursor;
        private final HugeLongArray walkLengths;
        private final long[] outputBuffer;

        private int currentIndex;

        CompressedWalkIterator(
            long startIndex,
            long endIndex,
            HugeObjectArray<byte[]> compressedWalks,
            HugeLongArray walkLengths,
            int maxWalkLength
        ) {
            this.walkLengths = walkLengths;

            this.cursor = compressedWalks.newCursor();
            compressedWalks.initCursor(cursor, startIndex, endIndex + 1);
            cursor.next();
            currentIndex = cursor.offset;

            outputBuffer = new long[maxWalkLength];
        }

        @Override
        public boolean hasNext() {
            if (currentIndex < cursor.limit) return true;

            if (cursor.next()) {
                currentIndex = cursor.offset;
                return true;
            }

            return false;
        }

        /**
         * Returns the next random walk in the specified range.
         * The long array returned by this method will be reused in the following call to `next` and must not be shared.
         * If the current walk is shorter than the maximum walk length, the remaining elements will be filled with -1.
         */
        @Override
        public long[] next() {
            var compressedWalk = cursor.array[currentIndex];
            var walkLength = (int) walkLengths.get(currentIndex);
            Arrays.fill(outputBuffer, -1L);
            ZigZagLongDecoding.zigZagUncompress(compressedWalk, walkLength, outputBuffer);

            currentIndex++;
            return outputBuffer;
        }
    }
}
