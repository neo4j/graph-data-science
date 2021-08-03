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


import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryRange;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.core.utils.paged.SparseLongArray.NOT_FOUND;

class SparseLongArrayTest {

    @Test
    void testEmpty() {
        var array = SparseLongArray.builder(42).build();
        assertEquals(NOT_FOUND, array.toMappedNodeId(23));
    }

    @Test
    void testZeroEntry() {
        var builder = SparseLongArray.builder(42);
        builder.set(0, 0);
        assertEquals(0, builder.build().toMappedNodeId(0));
    }

    @Test
    void testSingleEntry() {
        var builder = SparseLongArray.builder(42);
        builder.set(0, 23);
        assertEquals(0, builder.build().toMappedNodeId(23));
    }

    @Test
    void testMultipleEntries() {
        var capacity = 128;
        var builder = SparseLongArray.builder(capacity);
        for (var input : inputChunks(capacity, 2)) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }
        var array = builder.build();
        for (int i = 0; i < capacity; i += 2) {
            assertEquals(i / 2, array.toMappedNodeId(i), formatWithLocale("wrong mapping for original id %d", i));
        }
    }

    @Test
    void testContains() {
        var capacity = 8420;

        var builder = SparseLongArray.builder(capacity);
        for (var input : inputChunks(capacity, 7)) {
            builder.set(input.allocationIndex(), input.inputChunk());
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

        var builder = SparseLongArray.builder(capacity);
        for (var input : inputChunks(capacity, 7)) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }

        var array = builder.build();
        for (int i = 0; i < capacity; i += 7) {
            assertEquals(i / 7, array.toMappedNodeId(i), formatWithLocale("wrong mapping for original id %d", i));
        }
    }

    @Test
    void testNonExisting() {
        var builder = SparseLongArray.builder(42);
        builder.set(0, 23);
        assertEquals(NOT_FOUND, builder.build().toMappedNodeId(24));
    }

    @Test
    void testForwardMapping() {
        var builder = SparseLongArray.builder(42);
        builder.set(0, 23);
        assertEquals(23, builder.build().toOriginalNodeId(0));
    }

    @Test
    void testForwardMappingNonExisting() {
        var builder = SparseLongArray.builder(42);
        builder.set(0, 23);
        assertEquals(0, builder.build().toOriginalNodeId(1));
    }

    @Test
    void testForwardMappingWithBlockEntries() {
        var capacity = 8420;

        var builder = SparseLongArray.builder(capacity);
        for (var input : inputChunks(capacity, 11)) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }

        var array = builder.build();

        for (int i = 0; i < capacity / 11; i++) {
            assertEquals(i * 11, array.toOriginalNodeId(i), formatWithLocale("wrong original id for mapped id %d", i));
        }
    }

    @Test
    void testForwardMappingWithBlockEntriesNotFound() {
        var capacity = 8420;

        var builder = SparseLongArray.builder(capacity);
        for (var input : inputChunks(capacity, 13)) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }

        var array = builder.build();

        var nonExistingId = (capacity / 13) + 1;

        for (int i = nonExistingId; i < capacity; i++) {
            assertEquals(0, array.toOriginalNodeId(i), formatWithLocale("unexpected original id for mapped id %d", i));
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("batches")
    void testIdCount(long expectedIdCount, InputChunksAndAllocationIndex[] inputs) {
        var builder = SparseLongArray.builder(10_000);

        for (var input : inputs) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }

        var sparseLongArray = builder.build();
        assertEquals(expectedIdCount, sparseLongArray.idCount());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("batches")
    void testSetBatch(@SuppressWarnings("unused") long expectedIdCount, InputChunksAndAllocationIndex[] inputs) {
        var builder = SparseLongArray.builder(10_000);

        for (var input : inputs) {
            builder.set(input.allocationIndex(), input.inputChunk());
        }

        var array = builder.build();

        for (var input : inputs) {
            var chunk = input.inputChunk();
            for (int i = 0; i < chunk.length; i++) {
                var originalId = chunk[i];
                var mappedId = input.allocationIndex() + i;
                var actualOriginalId = array.toOriginalNodeId(mappedId);
                var actualMappedId = array.toMappedNodeId(originalId);
                assertEquals(
                    originalId,
                    actualOriginalId,
                    formatWithLocale("wrong original id for mapped id %d", mappedId)
                );
                assertEquals(
                    mappedId,
                    actualMappedId,
                    formatWithLocale("wrong mapped id for original id %d", originalId)
                );
            }
        }
    }

    static Stream<Arguments> batches() {
        return Stream.of(
            inputFromChunks(),
            inputFromChunks(identityArray(1024, 0)),
            inputFromChunks(identityArray(1024, 4096)),
            inputFromChunks(identityArray(1024, 8192)),
            inputFromChunks(identityArray(4096, 0)),
            inputFromChunks(identityArray(4096, 4096)),
            inputFromChunks(identityArray(4096, 0), identityArray(5000 - 4096, 4096)),
            inputFromChunks(identityArray(4096, 0), identityArray(9999 - 8192, 8192)),
            inputFromChunks(identityArray(4096, 0)),
            inputFromChunks(identityArray(4096, 0), identityArray(4096, 4096), identityArray(9999 - 8192, 8192)),
            inputFromChunks(identityArray(4096, 4096), identityArray(9999 - 8192, 8192), identityArray(4096, 0))
        ).map(pair -> arguments(pair.getTwo(), pair.getOne()));
    }

    @Test
    void testNonConsecutiveAllocationIndex() {
        var capacity = 10_000;
        var remainingLength = 10_000 - 2 * 4096;

        var builder = SparseLongArray.builder(capacity);
        builder.set(0, identityArray(4096, 0));
        builder.set(4096 + remainingLength, identityArray(4096, 4096));
        builder.set(4096, identityArray(remainingLength, 2 * 4096));

        var array = builder.build();
        assertEquals(capacity, array.idCount());

        var originalIds = Stream.of(
            identityArray(4096, 0),
            identityArray(remainingLength, 2 * 4096),
            identityArray(4096, 4096)
        )
            .flatMap(chunk -> Arrays.stream(chunk).boxed())
            .iterator();

        for (long mappedId = 0; mappedId < capacity; mappedId++) {
            long originalId = array.toOriginalNodeId(mappedId);
            assertEquals(
                originalIds.next(),
                originalId,
                formatWithLocale("wrong original id for mapped id %d", mappedId)
            );
        }
    }

    @ParameterizedTest
    @CsvSource({
        "10000,1432",
        "100000,13120",
        "1000000,130024",
        "1000000000000,129882812640"
    })
    void memoryEstimation(long highestNeoId, long expectedBytes) {
        var dimensions = ImmutableGraphDimensions.builder().nodeCount(0).highestNeoId(highestNeoId).build();
        // does not affect SLA memory estimation
        var concurrency = 42;

        assertEquals(
            MemoryRange.of(expectedBytes),
            SparseLongArray.memoryEstimation().estimate(dimensions, concurrency).memoryUsage()
        );
    }

    @Test
    void testValueLargerThanIntMax() {
        var maxId = ((long) Integer.MAX_VALUE) + 1;
        var builder = SparseLongArray.builder(maxId + 1);
        builder.set(0, maxId);
        var array = builder.build();
        assertEquals(0, array.toMappedNodeId(maxId));
        assertEquals(maxId, array.toOriginalNodeId(0));
    }

    @ParameterizedTest
    @MethodSource("stridesAndChunkSizes")
    void testSetParallel(int stride, int blocksPerChunk) {
        int capacity = 100_000;
        var builder = SparseLongArray.builder(capacity);

        var chunks = Arrays.asList(inputChunks(capacity, stride, blocksPerChunk));
        Collections.shuffle(chunks, new Random(42));
        var chunkQueue = new ArrayBlockingQueue<>(chunks.size(), true, chunks);

        var tasks = IntStream
            .range(0, ConcurrencyConfig.DEFAULT_CONCURRENCY)
            .mapToObj(i -> new SetTask(chunkQueue, builder))
            .collect(toList());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var array = builder.build();

        var originalIds = chunks.stream()
            .sorted(Comparator.comparingLong(InputChunksAndAllocationIndex::allocationIndex))
            .flatMap(chunk -> Arrays.stream(chunk.inputChunk()).boxed())
            .iterator();

        for (long mappedId = 0; mappedId < (capacity / stride); mappedId++) {
            long originalId = array.toOriginalNodeId(mappedId);
            assertEquals(
                originalIds.next(),
                originalId,
                formatWithLocale("wrong original id for mapped id %d", mappedId)
            );
        }
    }

    static Stream<Arguments> stridesAndChunkSizes() {
        return TestSupport.crossArguments(
            () -> Stream.of(1, 2, 3, 8, 13, 37, 42).map(Arguments::arguments),
            () -> Stream.of(1, 2, 3, 5, 8, 13).map(Arguments::arguments)
        );
    }

    static class SetTask implements Runnable {
        private final Queue<InputChunksAndAllocationIndex> queue;
        private final SparseLongArray.Builder builder;

        SetTask(Queue<InputChunksAndAllocationIndex> queue, SparseLongArray.Builder builder) {
            this.queue = queue;
            this.builder = builder;
        }

        @Override
        public void run() {
            while (true) {
                var chunk = queue.poll();
                if (chunk == null) {
                    return;
                }
                var allocationIndex = chunk.allocationIndex();
                builder.set(allocationIndex, chunk.inputChunk());
            }
        }
    }

    private static long[] identityArray(int length, long offset) {
        var array = new long[length];
        Arrays.setAll(array, i -> i + offset);
        return array;
    }

    @ValueClass
    interface InputChunksAndAllocationIndex {
        long allocationIndex();

        long[] inputChunk();
    }

    private static InputChunksAndAllocationIndex[] inputChunks(int capacity, int stride) {
        return inputChunks(capacity, stride, 1);
    }

    private static InputChunksAndAllocationIndex[] inputChunks(int capacity, int stride, int blocksPerChunk) {
        var chunkSize = SparseLongArray.BLOCK_SIZE * Long.SIZE * blocksPerChunk;
        return LongStream
            .iterate(0, i -> i < capacity, i -> i + stride)
            .boxed().collect(collectingAndThen(
                groupingBy(i -> i / chunkSize),
                groups -> groups.values().stream()
            ))
            .map(values -> values.stream().mapToLong(Long::longValue).toArray())
            .map(new IntoChunks())
            .toArray(InputChunksAndAllocationIndex[]::new);
    }

    private static Pair<InputChunksAndAllocationIndex[], Long> inputFromChunks(long[]... inputs) {
        var folder = new IntoChunks();
        var chunks = Arrays.stream(inputs).map(folder).toArray(InputChunksAndAllocationIndex[]::new);
        var count = folder.builder.inputChunk().build().allocationIndex();
        return Tuples.pair(chunks, count);
    }

    private static final class IntoChunks implements Function<long[], InputChunksAndAllocationIndex> {
        private final ImmutableInputChunksAndAllocationIndex.Builder builder =
            ImmutableInputChunksAndAllocationIndex.builder().allocationIndex(0);

        @Override
        public InputChunksAndAllocationIndex apply(long[] inputChunk) {
            var chunk = builder.inputChunk(inputChunk).build();
            builder.allocationIndex(chunk.allocationIndex() + inputChunk.length);
            return chunk;
        }
    }
}
