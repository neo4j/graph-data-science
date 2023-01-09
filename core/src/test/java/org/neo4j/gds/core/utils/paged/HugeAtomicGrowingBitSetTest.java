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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.utils.paged.HugeAtomicGrowingBitSet.PAGE_SHIFT_BITS;

class HugeAtomicGrowingBitSetTest {

    static Stream<Arguments> bitsets() {
        return Stream.of(
            // empty bit set
            Arguments.of(HugeAtomicGrowingBitSet.create(0)),
            // bit set of 3 pages
            Arguments.of(HugeAtomicGrowingBitSet.create(2 * (1L << PAGE_SHIFT_BITS) + 42))
        );
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testSet(HugeAtomicGrowingBitSet bitSet) {
        testSetAssert(bitSet, 23); // page 0
        testSetAssert(bitSet, (1L << PAGE_SHIFT_BITS) + 23); // page 1
        testSetAssert(bitSet, 2 * (1L << PAGE_SHIFT_BITS) + 23); // page 2
    }

    private static void testSetAssert(HugeAtomicGrowingBitSet bitSet, long index) {
        assertThat(bitSet.get(index)).isFalse();
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testGetAndSet(HugeAtomicGrowingBitSet bitSet) {
        testGetAndSetAssert(bitSet, 23, 42); // page 0
        testGetAndSetAssert(bitSet, (1L << PAGE_SHIFT_BITS) + 23, (1L << PAGE_SHIFT_BITS) + 42); // page 1
        testGetAndSetAssert(bitSet, 2 * (1L << PAGE_SHIFT_BITS) + 23, 2 * (1L << PAGE_SHIFT_BITS) + 42); // page 2
    }

    private static void testGetAndSetAssert(HugeAtomicGrowingBitSet bitSet, long idx0, long idx1) {
        // getAndSet a bit that is currently false
        assertThat(bitSet.get(idx0)).isFalse();
        assertThat(bitSet.getAndSet(idx0)).isFalse();
        assertThat(bitSet.get(idx0)).isTrue();
        // getAndSet a bit that is currently true
        bitSet.set(idx1);
        assertThat(bitSet.get(idx1)).isTrue();
        assertThat(bitSet.getAndSet(idx1)).isTrue();
        assertThat(bitSet.get(idx1)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testCardinality(HugeAtomicGrowingBitSet bitSet) {
        assertThat(bitSet.cardinality()).isEqualTo(0);
        bitSet.set(23); // page 0
        assertThat(bitSet.cardinality()).isEqualTo(1);
        bitSet.set((1L << PAGE_SHIFT_BITS) + 23); // page 1
        assertThat(bitSet.cardinality()).isEqualTo(2);
        bitSet.set(2 * (1L << PAGE_SHIFT_BITS) + 23); // page 2
        assertThat(bitSet.cardinality()).isEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testClearAtIndex(HugeAtomicGrowingBitSet bitSet) {
        assertThat(bitSet.cardinality()).isEqualTo(0);
        testClearAtIndexAssert(bitSet, 23); // page 0
        testClearAtIndexAssert(bitSet, (1L << PAGE_SHIFT_BITS) + 23); // page 1
        testClearAtIndexAssert(bitSet, 2 * (1L << PAGE_SHIFT_BITS) + 23); // page 2
    }

    private static void testClearAtIndexAssert(HugeAtomicGrowingBitSet bitSet, long index) {
        bitSet.set(index);
        assertThat(bitSet.cardinality()).isEqualTo(1);
        bitSet.clear(index);
        assertThat(bitSet.cardinality()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testForEachSetBit(HugeAtomicGrowingBitSet bitSet) {
        var rng = ThreadLocalRandom.current();
        long bound = 42 * (1L << PAGE_SHIFT_BITS) + 42;

        var expectedSetBits = IntStream.range(0, 10_000)
            .mapToLong(__ -> rng.nextLong(0, bound))
            .boxed()
            .peek(bitSet::set)
            .collect(Collectors.toSet());

        var actualSetBits = new HashSet<Long>();
        bitSet.forEachSetBit(actualSetBits::add);

        assertThat(actualSetBits).isEqualTo(expectedSetBits);
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testSetParallel(HugeAtomicGrowingBitSet bitSet) {
        int concurrency = 8;
        int nodeCount = 1_000_000;

        List<Runnable> tasks = PartitionUtils.rangePartition(concurrency, nodeCount, (partition) -> () -> {
            long startNode = partition.startNode();
            long endNode = partition.startNode() + partition.nodeCount();
            for (var i = startNode; i < endNode; i++) {
                bitSet.set(i);
            }
        }, Optional.empty());

        RunWithConcurrency
            .builder()
            .tasks(tasks)
            .concurrency(concurrency)
            .build()
            .run();

        assertThat(bitSet.cardinality()).isEqualTo(nodeCount);
    }
}
