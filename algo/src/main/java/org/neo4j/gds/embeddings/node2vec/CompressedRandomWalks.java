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

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.gds.core.compression.common.ZigZagLongDecoding;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.Iterator;

import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.zigZag;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CompressedRandomWalks {
    private final HugeObjectArray<byte[]> compressedWalks;
    private final HugeIntArray walkLengths;

    private int maxWalkLength;
    private long size = 0L;

    public CompressedRandomWalks(long maxWalkCount) {
        this.compressedWalks = HugeObjectArray.newArray(byte[].class, maxWalkCount);
        this.walkLengths = HugeIntArray.newArray(maxWalkCount);
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
        return walkLengths.get(index);
    }

    public static class CompressedWalkIterator extends AbstractIterator<long[]> {
        private final HugeCursor<byte[][]> cursor;
        private final HugeIntArray walkLengths;
        private final long[] outputBuffer;

        private int currentIndex;

        CompressedWalkIterator(
            long startIndex,
            long endIndex,
            HugeObjectArray<byte[]> compressedWalks,
            HugeIntArray walkLengths,
            int maxWalkLength
        ) {
            this.walkLengths = walkLengths;

            this.cursor = compressedWalks.newCursor();
            compressedWalks.initCursor(cursor, startIndex, endIndex + 1);
            cursor.next();
            currentIndex = cursor.offset;

            outputBuffer = new long[maxWalkLength];
        }

        /**
         * Returns the next random walk in the specified range.
         * The long array returned by this method will be reused in the following call to `next` and must not be shared.
         * If the current walk is shorter than the maximum walk length, the remaining elements will be filled with -1.
         */
        @Override
        protected long[] fetch() {
            if (currentIndex >= cursor.limit) {
                if (!cursor.next()) {
                    return done();
                }
                currentIndex = cursor.offset;
            }

            var compressedWalk = cursor.array[currentIndex];
            var walkLength = walkLengths.get(cursor.base + currentIndex);
            Arrays.fill(outputBuffer, -1L);
            ZigZagLongDecoding.zigZagUncompress(compressedWalk, walkLength, outputBuffer);

            currentIndex++;
            return outputBuffer;
        }
    }
}
