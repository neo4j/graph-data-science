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

public final class SparseLongArray {

    public static final long NOT_FOUND = -1;

    static final int BLOCK_SIZE = 64;
    private static final int BLOCK_SHIFT = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    private static final int BLOCK_MASK = BLOCK_SIZE - 1;

    // Number of mapped ids. This is set via the builder.
    private final long idCount;

    // Each element (long) represents a page.
    // Each page represents 64 possible ids.
    private final long[] array;

    // Each block represents BLOCK_SIZE pages. Each entry
    // stores the number of ids mapped within all
    // preceding blocks. This is set via the builder.
    private final long[] blockCounts;

    public static Builder builder(long capacity) {
        return new Builder(capacity);
    }

    private SparseLongArray(long idCount, long[] array, long[] blockCounts) {
        this.idCount = idCount;
        this.array = array;
        this.blockCounts = blockCounts;
    }

    public long idCount() {
        return idCount;
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

    public boolean contains(long originalId) {
        var page = (int) (originalId >>> BLOCK_SHIFT);
        var indexInPage = originalId & BLOCK_MASK;
        // Check if original id is contained
        long mask = 1L << indexInPage;
        return (mask & array[page]) == mask;
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

    @TestOnly
    MethodHandles.Lookup lookup() {
        return MethodHandles.lookup();
    }

    public static class Builder {

        private final AutoCloseableThreadLocal<ThreadLocalBuilder> localBuilders;
        private final SparseLongArrayCombiner combiner;

        Builder(long capacity) {
            this.combiner = new SparseLongArrayCombiner(capacity);
            this.localBuilders = new AutoCloseableThreadLocal<>(
                () -> new ThreadLocalBuilder(capacity),
                Optional.of(combiner)
            );
        }

        //        @TestOnly
        public void set(long originalId) {
            localBuilders.get().set(originalId);
        }

        public void set(long[] originalIds, int offset, int length) {
            var array = localBuilders.get();
            for (int i = offset; i < offset + length; i++) {
                // TODO: radix sort approach
                array.set(originalIds[i]);
            }
        }

        public SparseLongArray build() {
            localBuilders.close();

            return computeCounts(combiner.build());
        }

        private SparseLongArray computeCounts(ThreadLocalBuilder sparseLongArray) {
            long[] array = sparseLongArray.array;
            int size = array.length;
            int cappedSize = size - BLOCK_SIZE;
            // blockCounts[0] is always 0, hence + 1
            long[] blockCounts = new long[(size >>> BLOCK_SHIFT) + 1];
            ;

            long count = 0;
            int block;
            for (block = 0; block < cappedSize; block += BLOCK_SIZE) {
                for (int page = block; page < block + BLOCK_SIZE; page++) {
                    count += Long.bitCount(array[page]);
                }
                blockCounts[(block >>> BLOCK_SHIFT) + 1] = count;
            }

            // Count the remaining ids in the tail.
            for (int page = block; page < size; page++) {
                count += Long.bitCount(array[page]);
            }

            return new SparseLongArray(count, array, blockCounts);
        }

        private static class ThreadLocalBuilder implements AutoCloseable {

            private final long[] array;

            ThreadLocalBuilder(long capacity) {
                var size = (int) BitUtil.ceilDiv(capacity, Long.SIZE);
                this.array = new long[size];
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
        }

        static class SparseLongArrayCombiner implements Consumer<ThreadLocalBuilder> {

            private final long capacity;
            private ThreadLocalBuilder result;

            SparseLongArrayCombiner(long capacity) {
                this.capacity = capacity;
            }

            @Override
            public void accept(ThreadLocalBuilder other) {
                if (result == null) {
                    result = other;
                } else {
                    int size = result.array.length;
                    for (int page = 0; page < size; page++) {
                        result.array[page] |= other.array[page];
                    }
                }
            }

            ThreadLocalBuilder build() {
                return result != null ? result : new ThreadLocalBuilder(capacity);
            }
        }
    }

}
