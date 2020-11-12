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

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.AscendingLongComparator;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.utils.AutoCloseableThreadLocal;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

public final class SparseLongArray {

    public static final long NOT_FOUND = -1;

    public static final int BLOCK_SIZE = 64;
    public static final int SUPER_BLOCK_SIZE = BLOCK_SIZE * Long.SIZE;
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
    private final long[] allocationIndexes;
    private final long[] sortedAllocationIndexes;
    private final int[] allocationToBlockIndexes;

    public static Builder builder(long capacity) {
        return new Builder(capacity);
    }

    public static SequentialBuilder sequentialBuilder(long capacity) {
        return new SequentialBuilder(capacity);
    }

    private SparseLongArray(
        long idCount,
        long[] array,
        long[] allocationIndexes,
        long[] sortedAllocationIndexes,
        int[] allocationToBlockIndexes
    ) {
        this.idCount = idCount;
        this.array = array;
        this.allocationIndexes = allocationIndexes;
        this.sortedAllocationIndexes = sortedAllocationIndexes;
        this.allocationToBlockIndexes = allocationToBlockIndexes;
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
        var mappedId = allocationIndexes[block];
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
        var startBlockIndex = ArrayUtil.binaryLookup(mappedId, sortedAllocationIndexes);
        startBlockIndex = allocationToBlockIndexes[startBlockIndex];
        var array = this.array;
        var blockStart = startBlockIndex << BLOCK_SHIFT;
        var blockEnd = Math.min((startBlockIndex + 1) << BLOCK_SHIFT, array.length);
        var originalId = allocationIndexes[startBlockIndex];
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

        // TODO: could be shared singleton-ish thing
//        private final AutoCloseableThreadLocal<ThreadLocalBuilder> localBuilders;
//        private final SparseLongArrayCombiner combiner;

        private final long[] array;
        private final long[] allocationIndexes;
        // good luck figuring out how and why this works :)
//        private long countsOfLastSeenBlock;

        Builder(long capacity) {
//            this.combiner = new SparseLongArrayCombiner(capacity);
//            this.localBuilders = new AutoCloseableThreadLocal<>(
//                () -> new ThreadLocalBuilder(capacity),
//                Optional.of(combiner)
//            );

            var size = (int) BitUtil.ceilDiv(capacity, Long.SIZE);
            this.array = new long[size];
            this.allocationIndexes = new long[(size >>> BLOCK_SHIFT) + 1];
            Arrays.fill(allocationIndexes, Long.MAX_VALUE);
        }

        @TestOnly
        void set(long allocationIndex, long... originalIds) {
            set(allocationIndex, originalIds, 0, originalIds.length);
        }

        public void set(long allocationIndex, long[] originalIds, int offset, int length) {
            var array = this;
            var prevBlock = -1;
            var prevCount = 0;

            // Can we find something better than checking at every value?
            for (int i = 0; i < length; i++) {
                var originalId = originalIds[i + offset];
                var block = (int) (originalId >>> 12); // BLOCK_SIZE * Long.SIZE
                if (block != prevBlock) {
                    if (prevBlock != -1) {
                        assert array.allocationIndexes[prevBlock] == Long.MAX_VALUE;
                        array.allocationIndexes[prevBlock] = prevCount + allocationIndex;
                        //                        allocationIndex += i;
                    }
                    prevBlock = block;
                    prevCount = i;
                }
                array.set(originalId);
            }
//            if (length > 0) {
            assert array.allocationIndexes[prevBlock] == Long.MAX_VALUE;
            array.allocationIndexes[prevBlock] = prevCount + allocationIndex;
            //            array.setCountsOfLastSeenBlock(length - prevCount);
//            }
        }

        public SparseLongArray build() {
            return computeCounts();
        }

        private void set(long originalId) {
            var page = (int) (originalId >>> BLOCK_SHIFT);
            var indexInPage = originalId & BLOCK_MASK;
            var mask = 1L << indexInPage;
            array[page] |= mask;
        }

        private SparseLongArray computeCounts() {
            long[] array = this.array;
            int size = array.length;

            var allocationIndexes = this.allocationIndexes;
            var sortedArrayIndexes = IndirectSort.mergesort(
                0,
                allocationIndexes.length,
                new AscendingLongComparator(allocationIndexes)
            );
            var sortedAllocationIndexes = new long[allocationIndexes.length];
            Arrays.setAll(sortedAllocationIndexes, i -> allocationIndexes[sortedArrayIndexes[i]]);

            int lastSortedAllocationIndex = sortedAllocationIndexes.length - 1;
            long count = 0;
            while (lastSortedAllocationIndex > 0) {
                var lastCount = sortedAllocationIndexes[lastSortedAllocationIndex];
                if (lastCount != Long.MAX_VALUE) {
                    count = lastCount;
                    break;
                }
                --lastSortedAllocationIndex;
            }

            var lastSortedBlock = sortedArrayIndexes[lastSortedAllocationIndex];
            // Count the remaining ids in the last block.
            var lastBlockBegin = lastSortedBlock << BLOCK_SHIFT;
            var lastBlockEnd = Math.min(size, lastBlockBegin + BLOCK_SIZE);
            for (int page = lastBlockBegin; page < lastBlockEnd; page++) {
                count += Long.bitCount(array[page]);
            }

            return new SparseLongArray(count, array, allocationIndexes, sortedAllocationIndexes, sortedArrayIndexes);
        }

        private static class ThreadLocalBuilder implements AutoCloseable {

            private final long[] array;
            private final long[] allocationIndexes;
            private long countsOfLastSeenBlock;

            ThreadLocalBuilder(long capacity) {
                var size = (int) BitUtil.ceilDiv(capacity, Long.SIZE);
                this.array = new long[size];
                this.allocationIndexes = new long[(size >>> BLOCK_SHIFT) + 1];
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

            void setCountsFor(int block, long allocationIndex) {
                allocationIndexes[block] = allocationIndex;
            }

            void setCountsOfLastSeenBlock(long count) {
                countsOfLastSeenBlock = count;
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

    public static class SequentialBuilder {

        // TODO: could be shared singleton-ish thing
        private final AutoCloseableThreadLocal<ThreadLocalBuilder> localBuilders;
        private final SparseLongArrayCombiner combiner;

        SequentialBuilder(long capacity) {
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

            long count = 0;
            int block;
            for (block = 0; block < cappedSize; block += BLOCK_SIZE) {
                for (int page = block; page < block + BLOCK_SIZE; page++) {
                    count += Long.bitCount(array[page]);
                }
                blockCounts[(block >>> BLOCK_SHIFT) + 1] = count;
            }

            // Count the remaining ids in the tail.
            var lastBlockBegin = size - (size & BLOCK_MASK);
            for (int page = lastBlockBegin; page < size; page++) {
                count += Long.bitCount(array[page]);
            }

            var sortedArrayIndexes = new int[blockCounts.length];
            Arrays.setAll(sortedArrayIndexes, i -> i);

            return new SparseLongArray(count, array, blockCounts, blockCounts, sortedArrayIndexes);
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
