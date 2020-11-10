/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.function.Consumer;

public final class SparseLongArray implements AutoCloseable {

    public static final long NOT_FOUND = -1;

    static final int BLOCK_SIZE = 64;
    private static final int BLOCK_SHIFT = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    private static final int BLOCK_MASK = BLOCK_SIZE - 1;

    private final int size;

    private final long[] array;
    private final long[] blockCounts;

    public static Builder create(long capacity) {
        return new Builder(capacity);
    }

    private SparseLongArray(long capacity) {
        this.size = (int) BitUtil.ceilDiv(capacity, Long.SIZE);
        this.array = new long[size];
        // blockCounts[0] is always 0, hence + 1
        this.blockCounts = new long[(size >>> BLOCK_SHIFT) + 1];
    }

    public long toMappedNodeId(long originalId) {
        var page = (int) (originalId >>> BLOCK_SHIFT);
        var indexInPage = originalId & BLOCK_MASK;

        // Check if original id is contained
        long mask = 1L << indexInPage;
        if ((mask & array[page]) != mask) {
            return NOT_FOUND;
        }

        var block = page >>> BLOCK_SHIFT;
        // Get the count from all previous blocks
        var mappedId = blockCounts[block];
        // Count set bits up to original id
        var a = array;
        // Get count within current block
        for (int pageIdx = page & ~BLOCK_MASK; pageIdx < page; pageIdx++) {
            mappedId += Long.bitCount(a[pageIdx]);
        }
        // tail (long at offset)
        var shift = Long.SIZE - indexInPage - 1;
        mappedId += Long.bitCount(a[page] << shift);

        return mappedId - 1;
    }

    public long toOriginalNodeId(long mappedId) {
        var startBlockIndex = ArrayUtil.binaryLookup(mappedId, blockCounts);
        var array = this.array;
        var blockStart = startBlockIndex << BLOCK_SHIFT;
        var blockEnd = Math.min((startBlockIndex + 1) << BLOCK_SHIFT, array.length);
        var originalId = blockCounts[startBlockIndex];
        for (int blockIndex = blockStart; blockIndex < blockEnd; blockIndex++) {
            var page = array[blockIndex];
            var pos = 0;
            var idsInPage = Long.bitCount(page);
            if (originalId + idsInPage > mappedId) {
                while (originalId <= mappedId) {
                    if ((page & 1) == 1) {
                        ++originalId;
                    }
                    page >>>= 1;
                    ++pos;
                }
                return (blockIndex << BLOCK_SHIFT) + (pos - 1);
            }
            originalId += idsInPage;
        }
        // Returning 0, since this is what the current
        // IdMap implementation returns in that case.
        return 0;
    }

    @Override
    public void close() {
    }

    private void set(long originalId) {
        var page = (int) (originalId >>> BLOCK_SHIFT);
        var indexInPage = originalId & BLOCK_MASK;
        var mask = 1L << indexInPage;
        array[page] |= mask;
    }

    @TestOnly
    MethodHandles.Lookup lookup() {
        return MethodHandles.lookup();
    }

    static class SparseLongArrayCombiner implements Consumer<SparseLongArray> {

        private final long capacity;
        private SparseLongArray result;

        SparseLongArrayCombiner(long capacity) {
            this.capacity = capacity;
        }

        @Override
        public void accept(SparseLongArray other) {
            if (result == null) {
                result = other;
            } else {
                int size = result.size;
                for (int page = 0; page < size; page++) {
                    result.array[page] |= other.array[page];
                }
            }
        }

        SparseLongArray build() {
            return result != null ? result : new SparseLongArray(capacity);
        }
    }

    public static class Builder {

        private final AutoCloseableThreadLocal<SparseLongArray> localArray;
        private final SparseLongArrayCombiner combiner;

        Builder(long capacity) {
            this.combiner = new SparseLongArrayCombiner(capacity);
            this.localArray = new AutoCloseableThreadLocal<>(
                () -> new SparseLongArray(capacity),
                Optional.of(combiner)
            );
        }

        @TestOnly
        void set(long originalId) {
            localArray.get().set(originalId);
        }

        public void set(long[] originalIds, int offset, int length) {
            var array = localArray.get();
            for (int i = offset; i < offset + length; i++) {
                // TODO: radix sort approach
                array.set(originalIds[i]);
            }
        }

        public SparseLongArray build() {
            localArray.close();
            return computeCounts(combiner.build());
        }

        private SparseLongArray computeCounts(SparseLongArray sparseLongArray) {
            int size = sparseLongArray.size - BLOCK_SIZE;
            long[] array = sparseLongArray.array;
            long[] blockCounts = sparseLongArray.blockCounts;

            long count = 0;
            for (int block = 0; block < size; block += BLOCK_SIZE) {
                for (int page = block; page < block + BLOCK_SIZE; page++) {
                    count += Long.bitCount(array[page]);
                }
                blockCounts[(block >>> BLOCK_SHIFT) + 1] = count;
            }

            return sparseLongArray;
        }
    }

}
