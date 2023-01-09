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
import static org.neo4j.gds.core.utils.paged.HugeAtomicPagedBitSet.PAGE_SHIFT_BITS;

class HugeAtomicPagedBitSetTest {

    static Stream<Arguments> bitsets() {
        return Stream.of(
            // empty bit set
            Arguments.of(HugeAtomicPagedBitSet.create(0)),
            // bit set of 3 pages
            Arguments.of(HugeAtomicPagedBitSet.create(2 * (1L << PAGE_SHIFT_BITS) + 42))
        );
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testSet(HugeAtomicPagedBitSet bitSet) {
        // page 0
        long index = 23;
        assertThat(bitSet.get(index)).isFalse();
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
        // page 1
        index = (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(bitSet.get(index)).isFalse();
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
        // page 2
        index = 2 * (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(bitSet.get(index)).isFalse();
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testGetAndSet(HugeAtomicPagedBitSet bitSet) {
        // page 0
        // getAndSet a bit that is currently false
        long index = 23;
        assertThat(bitSet.get(index)).isFalse();
        assertThat(bitSet.getAndSet(index)).isFalse();
        assertThat(bitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = 42;
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
        assertThat(bitSet.getAndSet(index)).isTrue();
        assertThat(bitSet.get(index)).isTrue();
        // page 1
        // getAndSet a bit that is currently false
        index = (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(bitSet.get(index)).isFalse();
        assertThat(bitSet.getAndSet(index)).isFalse();
        assertThat(bitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = (1L << PAGE_SHIFT_BITS) + 42;
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
        assertThat(bitSet.getAndSet(index)).isTrue();
        assertThat(bitSet.get(index)).isTrue();
        // page 2
        // getAndSet a bit that is currently false
        index = 2 * (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(bitSet.get(index)).isFalse();
        assertThat(bitSet.getAndSet(index)).isFalse();
        assertThat(bitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = 2 * (1L << PAGE_SHIFT_BITS) + 42;
        bitSet.set(index);
        assertThat(bitSet.get(index)).isTrue();
        assertThat(bitSet.getAndSet(index)).isTrue();
        assertThat(bitSet.get(index)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testCardinality(HugeAtomicPagedBitSet bitSet) {
        assertThat(bitSet.cardinality()).isEqualTo(0);
        // page 0
        bitSet.set(23);
        assertThat(bitSet.cardinality()).isEqualTo(1);
        // page 1
        bitSet.set((1L << PAGE_SHIFT_BITS) + 23);
        assertThat(bitSet.cardinality()).isEqualTo(2);
        // page 2
        bitSet.set(2 * (1L << PAGE_SHIFT_BITS) + 23);
        assertThat(bitSet.cardinality()).isEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testForEachSetBit(HugeAtomicPagedBitSet bitSet) {
        var rng = ThreadLocalRandom.current();
        long bound = 42 * (1L << PAGE_SHIFT_BITS) + 42;

        var expected = IntStream
            .range(0, 10_000)
            .mapToLong(__ -> rng.nextLong(0, bound))
            .boxed()
            .peek(bitSet::set)
            .collect(Collectors.toSet());

        var actual = new HashSet<Long>();
        bitSet.forEachSetBit(actual::add);

        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testSetParallel(HugeAtomicPagedBitSet bitSet) {
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
