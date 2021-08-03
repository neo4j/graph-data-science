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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.core.utils.paged.SparseLongArray.BLOCK_SIZE;
import static org.neo4j.gds.core.utils.paged.SparseLongArray.NOT_FOUND;

class SequentialSparseLongArrayTest {

    @Test
    void testEmpty() {
        var array = SparseLongArray.sequentialBuilder(42).build();
        assertEquals(NOT_FOUND, array.toMappedNodeId(23));
    }

    @Test
    void testZeroEntry() {
        var builder = SparseLongArray.sequentialBuilder(42);
        builder.set(0);
        assertEquals(0, builder.build().toMappedNodeId(0));
    }

    @Test
    void testSingleEntry() {
        var builder = SparseLongArray.sequentialBuilder(42);
        builder.set(23);
        assertEquals(0, builder.build().toMappedNodeId(23));
    }

    @Test
    void testMultipleEntries() {
        var capacity = 128;
        var builder = SparseLongArray.sequentialBuilder(capacity);
        for (int i = 0; i < capacity; i += 2) {
            builder.set(i);
        }
        var array = builder.build();
        for (int i = 0; i < capacity; i += 2) {
            assertEquals(i / 2, array.toMappedNodeId(i), formatWithLocale("wrong mapping for original id %d", i));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1024, 4096, 5000, 9999})
    void testIdCount(int expectedIdCount) {
        var builder = SparseLongArray.sequentialBuilder(10_000);
        for (int i = 0; i < expectedIdCount; i++) {
            builder.set(i);
        }
        assertEquals(expectedIdCount, builder.build().idCount());
    }

    @Test
    void testContains() {
        var capacity = 8420;

        var builder = SparseLongArray.sequentialBuilder(capacity);
        for (int i = 0; i < capacity; i += 7) {
            builder.set(i);
        }

        var array = builder.build();
        for (int originalId = 0; originalId < capacity; originalId++) {
            if (originalId % 7 == 0) {
                assertTrue(
                    array.contains(originalId),
                    formatWithLocale("original id %d should be contained", originalId)
                );
            } else {
                assertFalse(
                    array.contains(originalId),
                    formatWithLocale("original id %d should not be contained", originalId)
                );
            }
        }
    }

    @Test
    void testBlockEntries() {
        var capacity = 8420;

        var builder = SparseLongArray.sequentialBuilder(capacity);
        for (int i = 0; i < capacity; i += 7) {
            builder.set(i);
        }
        var array = builder.build();
        for (int i = 0; i < capacity; i += 7) {
            assertEquals(i / 7, array.toMappedNodeId(i), formatWithLocale("wrong mapping for original id %d", i));
        }
    }

    @Test
    void testNonExisting() {
        var builder = SparseLongArray.sequentialBuilder(42);
        builder.set(23);
        assertEquals(NOT_FOUND, builder.build().toMappedNodeId(24));
    }

    @Test
    void testForwardMapping() {
        var builder = SparseLongArray.sequentialBuilder(42);
        builder.set(23);
        assertEquals(23, builder.build().toOriginalNodeId(0));
    }

    @Test
    void testForwardMappingNonExisting() {
        var builder = SparseLongArray.sequentialBuilder(42);
        builder.set(23);
        assertEquals(0, builder.build().toOriginalNodeId(1));
    }

    @Test
    void testForwardMappingWithBlockEntries() {
        var capacity = 8420;

        var builder = SparseLongArray.sequentialBuilder(capacity);
        for (int i = 0; i < capacity; i += 11) {
            builder.set(i);
        }
        var array = builder.build();

        for (int i = 0; i < capacity / 11; i++) {
            assertEquals(i * 11, array.toOriginalNodeId(i), formatWithLocale("wrong original id for mapped id %d", i));
        }
    }

    @Test
    void testForwardMappingWithBlockEntriesNotFound() {
        var capacity = 8420;

        var builder = SparseLongArray.sequentialBuilder(capacity);
        for (int i = 0; i < capacity; i += 13) {
            builder.set(i);
        }
        var array = builder.build();

        var nonExistingId = (capacity / 13) + 1;

        for (int i = nonExistingId; i < capacity; i++) {
            assertEquals(0, array.toOriginalNodeId(i), formatWithLocale("unexpected original id for mapped id %d", i));
        }
    }

    @Test
    void testSetBatch() {
        var batch = LongStream.range(0, 10_000).toArray();
        var builder = SparseLongArray.sequentialBuilder(10_000);
        builder.set(batch, 5000, 5000);

        var array = builder.build();

        for (int i = 0; i < 5000; i++) {
            assertEquals(i + 5000, array.toOriginalNodeId(i), formatWithLocale("wrong original id for mapped id %d", i));
        }
    }

    @Test
    void testSetParallel() {
        int capacity = 10_000;
        int batchSize = 1_000;
        var builder = SparseLongArray.sequentialBuilder(capacity);
        var random = new SplittableRandom(42);

        var tasks = IntStream
            .range(0, 10)
            .mapToObj(i -> new SetTask(random.split(), builder, batchSize, capacity))
            .collect(Collectors.toList());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var array = builder.build();

        var originalIds = tasks
            .stream()
            .flatMap(t -> Arrays.stream(t.longs).boxed())
            .distinct()
            .sorted()
            .collect(Collectors.toList());

        long mappedIds = 0;
        for (long originalId = 0; originalId < capacity; originalId++) {
            long mappedId = array.toMappedNodeId(originalId);
            if (originalIds.contains(originalId)) {
                assertEquals(mappedIds++, mappedId, formatWithLocale("wrong mapped id for original id %d", originalId));
            } else {
                assertEquals(NOT_FOUND, mappedId);
            }
        }
    }

    static class SetTask implements Runnable {
        private final SparseLongArray.SequentialBuilder builder;

        private final long[] longs;

        SetTask(SplittableRandom random, SparseLongArray.SequentialBuilder builder, long size, int capacity) {
            this.builder = builder;
            this.longs = random.longs(size, 0, capacity).toArray();
        }

        @Override
        public void run() {
            builder.set(longs, 0, longs.length);
        }
    }

    public static void assertBlockCounts(SparseLongArray sparseLongArray) throws Throwable {
        var lookup = sparseLongArray.lookup();
        var array = (long[]) lookup.findGetter(SparseLongArray.class, "array", long[].class).invoke(sparseLongArray);
        var size = array.length;
        var actualBlockCounts = (long[]) lookup
            .findGetter(SparseLongArray.class, "sortedAllocationIndexes", long[].class)
            .invoke(sparseLongArray);
        var expectedBlockCounts = new long[size / BLOCK_SIZE + 1];

        int blocks = size - BLOCK_SIZE;

        long count = 0;
        for (int block = 0; block < blocks; block += BLOCK_SIZE) {
            for (int page = block; page < block + BLOCK_SIZE; page++) {
                count += Long.bitCount(array[page]);
            }
            expectedBlockCounts[block / BLOCK_SIZE + 1] = count;
        }

        assertArrayEquals(expectedBlockCounts, actualBlockCounts);
    }
}
