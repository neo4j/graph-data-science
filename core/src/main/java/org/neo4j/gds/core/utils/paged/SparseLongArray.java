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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.core.utils.ArrayLayout;
import org.neo4j.gds.core.utils.BitUtil;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;

public final class SparseLongArray {

    public static final long NOT_FOUND = NodeMapping.NOT_FOUND;

    public static final int BLOCK_SIZE = 64;
    public static final int SUPER_BLOCK_SIZE = BLOCK_SIZE * Long.SIZE;
    public static final int SUPER_BLOCK_SHIFT = Integer.numberOfTrailingZeros(SUPER_BLOCK_SIZE);
    private static final int BLOCK_SHIFT = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    private static final int BLOCK_MASK = BLOCK_SIZE - 1;

    // Number of mapped ids.
    private final long idCount;

    private final long highestNeoId;

    // Each element (long) represents a page.
    // Each page represents 64 possible ids.
    private final long[] array;


    // Each block represents BLOCK_SIZE pages.
    // Each value represents the id offset for all ids stored in the block.
    // Id ranges within a block to not overlap with id ranges in other blocks.
    // Block offsets are unordered as their insertion depends on the user of
    // the SLA (e.g. node loading does not insert blocks in a sequential order).
    private final long[] blockOffsets;
    // Sorted representation of block offsets to speed up the lookup for the
    // correct block using binary search using the Eytzinger search layout.
    private final long[] sortedBlockOffsets;
    // Maps block indices from the sorted offsets to the unsorted offsets.
    private final int[] blockMapping;

    public static int toValidBatchSize(int batchSize) {
        // We need to make sure that we scan aligned to the super block size, as we are not
        // allowed to write into the same block multiple times.
        return (int) BitUtil.align(batchSize, SUPER_BLOCK_SIZE);
    }

    public static Builder builder(long capacity) {
        return new Builder(capacity);
    }

    public static FromExistingBuilder fromExistingBuilder(long[] array) {
        return new FromExistingBuilder(array);
    }

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("sparse long array", (dimensions, concurrency) -> {
            var capacity = dimensions.highestPossibleNodeCount();
            var arraySize = BitUtil.ceilDiv(capacity, BLOCK_SIZE);
            var offsetSize = (arraySize >>> BLOCK_SHIFT) + 1;

            return MemoryEstimations
                .builder(SparseLongArray.class)
                .fixed("id array", MemoryUsage.sizeOfLongArray(arraySize))
                .fixed("block offsets", MemoryUsage.sizeOfLongArray(offsetSize))
                .fixed("sorted block offsets", MemoryUsage.sizeOfLongArray(offsetSize))
                .fixed("block mapping", MemoryUsage.sizeOfIntArray(offsetSize))
                .build();
        });
    }

    private SparseLongArray(
        long idCount,
        long highestNeoId,
        long[] array,
        long[] blockOffsets,
        long[] sortedBlockOffsets,
        int[] blockMapping
    ) {
        this.idCount = idCount;
        this.highestNeoId = highestNeoId;
        this.array = array;
        this.blockOffsets = blockOffsets;
        this.sortedBlockOffsets = sortedBlockOffsets;
        this.blockMapping = blockMapping;
    }

    public long idCount() {
        return idCount;
    }

    public long highestNeoId() {
        return highestNeoId;
    }

    public long toMappedNodeId(long originalId) {
        var page = pageId(originalId);
        var indexInPage = indexInPage(originalId);

        // Check if original id is contained
        long mask = 1L << indexInPage;
        if ((mask & array[page]) != mask) {
            return NOT_FOUND;
        }

        var block = page >>> BLOCK_SHIFT;
        // Get the id offset for that block
        var mappedId = blockOffsets[block];
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
        var page = pageId(originalId);
        var indexInPage = indexInPage(originalId);
        // Check if original id is contained
        long mask = 1L << indexInPage;
        return (mask & array[page]) == mask;
    }

    public long toOriginalNodeId(long mappedId) {
        var startBlockIndex = ArrayLayout.searchEytzinger(sortedBlockOffsets, mappedId);
        // since the eytzinger layout is 1-based and reserves the index 0 for the 'lower-than-lower-bound' case
        // and we know that we won't trigger that case, as our lower bound is 0
        // we subtract 1 to get to the actual index in the block mapping array.
        startBlockIndex = blockMapping[startBlockIndex - 1];
        var array = this.array;
        var blockStart = startBlockIndex << BLOCK_SHIFT;
        var blockEnd = Math.min((startBlockIndex + 1) << BLOCK_SHIFT, array.length);
        var originalId = blockOffsets[startBlockIndex];
        for (int blockIndex = blockStart; blockIndex < blockEnd; blockIndex++) {
            var page = array[blockIndex];
            var idsInPage = Long.bitCount(page);
            if (originalId + idsInPage > mappedId) {
                // Perform binary search within the page
                // to find the correct original id.
                var pos = 0;
                long mask = 0xFFFF_FFFFL;
                int shift = 32;

                while (shift > 0) {
                    var idsInLowerPage = Long.bitCount(page & mask);

                    if (originalId + idsInLowerPage > mappedId) {
                        page = page & mask;
                    } else {
                        pos += shift;
                        originalId += idsInLowerPage;
                        page = page >>> shift;
                    }

                    shift >>= 1;
                    mask >>= shift;
                }

                return (((long) blockIndex) << BLOCK_SHIFT) + pos;
            }
            originalId += idsInPage;
        }
        // Returning 0, since this is what the current
        // IdMap implementation returns in that case.
        return 0;
    }

    private static int pageId(long originalId) {
        return (int) (originalId >>> BLOCK_SHIFT);
    }

    private static long indexInPage(long originalId) {
        return originalId & BLOCK_MASK;
    }

    public static class Builder {

        private final long capacity;
        private final AutoCloseableThreadLocal<ThreadLocalBuilder> localBuilders;
        private final SparseLongArrayCombiner combiner;

        Builder(long capacity) {
            this.capacity = capacity;
            this.combiner = new SparseLongArrayCombiner(capacity);
            this.localBuilders = new AutoCloseableThreadLocal<>(
                () -> new ThreadLocalBuilder(capacity),
                Optional.of(combiner)
            );
        }

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
            return computeCounts(combiner.build().array);
        }

        protected SparseLongArray computeCounts(long[] array) {
            int size = array.length;
            int cappedSize = size - BLOCK_SIZE;
            // blockOffsets[0] is always 0, hence + 1
            long[] blockOffsets = new long[(size >>> BLOCK_SHIFT) + 1];

            // count block-wise on all first whole blocks and set offsets
            long count = 0;
            for (int block = 0; block <= cappedSize; block += BLOCK_SIZE) {
                for (int page = block; page < block + BLOCK_SIZE; page++) {
                    count += Long.bitCount(array[page]);
                }
                blockOffsets[(block >>> BLOCK_SHIFT) + 1] = count;
            }

            // Count the remaining ids in the tail.
            // bitwise AND will retain a value <= BLOCK_MASK
            var tailSize = size & BLOCK_MASK;
            for (int page = size - tailSize; page < size; page++) {
                count += Long.bitCount(array[page]);
            }

            // No need to sort as the blocks are already sorted.
            var blockMapping = new int[blockOffsets.length];
            Arrays.setAll(blockMapping, i -> i);
            var layouts = ArrayLayout.constructEytzinger(blockOffsets, blockMapping);

            // Capacity is set to include the highest node id.
            // The highest node id is therefore capacity - 1.
            var highestNeoId = capacity - 1;
            return new SparseLongArray(count, highestNeoId, array, blockOffsets, layouts.layout(), layouts.secondary());
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
                var page = pageId(originalId);
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

    public static class FromExistingBuilder extends Builder {

        private final long[] array;

        FromExistingBuilder(long[] array) {
            super(array.length);
            this.array = array;
        }

        @Override
        public SparseLongArray build() {
            return computeCounts(array);
        }
    }

}
