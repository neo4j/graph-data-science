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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.utils.paged.HugeAtomicPagedBitSet.PAGE_SHIFT_BITS;

class HugeAtomicPagedBitSetTest {

    public static Stream<Arguments> bitsets() {
        return Stream.of(
            // empty bit set
            Arguments.of(HugeAtomicPagedBitSet.create(0)),
            // bit set of 3 pages
            Arguments.of(HugeAtomicPagedBitSet.create(2 * (1L << PAGE_SHIFT_BITS) + 42))
        );
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testSet(HugeAtomicPagedBitSet atomicBitSet) {
        // page 0
        long index = 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
        // page 1
        index = (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
        // page 2
        index = 2 * (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testGetAndSet(HugeAtomicPagedBitSet atomicBitSet) {
        // page 0
        // getAndSet a bit that is currently false
        long index = 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        assertThat(atomicBitSet.getAndSet(index)).isFalse();
        assertThat(atomicBitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = 42;
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
        assertThat(atomicBitSet.getAndSet(index)).isTrue();
        assertThat(atomicBitSet.get(index)).isTrue();
        // page 1
        // getAndSet a bit that is currently false
        index = (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        assertThat(atomicBitSet.getAndSet(index)).isFalse();
        assertThat(atomicBitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = (1L << PAGE_SHIFT_BITS) + 42;
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
        assertThat(atomicBitSet.getAndSet(index)).isTrue();
        assertThat(atomicBitSet.get(index)).isTrue();
        // page 2
        // getAndSet a bit that is currently false
        index = 2 * (1L << PAGE_SHIFT_BITS) + 23;
        assertThat(atomicBitSet.get(index)).isFalse();
        assertThat(atomicBitSet.getAndSet(index)).isFalse();
        assertThat(atomicBitSet.get(index)).isTrue();
        // getAndSet a bit that is currently true
        index = 2 * (1L << PAGE_SHIFT_BITS) + 42;
        atomicBitSet.set(index);
        assertThat(atomicBitSet.get(index)).isTrue();
        assertThat(atomicBitSet.getAndSet(index)).isTrue();
        assertThat(atomicBitSet.get(index)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("bitsets")
    void testCardinality(HugeAtomicPagedBitSet atomicBitSet) {
        assertThat(atomicBitSet.cardinality()).isEqualTo(0);
        // page 0
        atomicBitSet.set(23);
        assertThat(atomicBitSet.cardinality()).isEqualTo(1);
        // page 1
        atomicBitSet.set((1L << PAGE_SHIFT_BITS) + 23);
        assertThat(atomicBitSet.cardinality()).isEqualTo(2);
        // page 2
        atomicBitSet.set(2 * (1L << PAGE_SHIFT_BITS) + 23);
        assertThat(atomicBitSet.cardinality()).isEqualTo(3);
    }

    @Test
    void writingAndGrowingShouldBeThreadSafe() {
        int concurrency = 8;
        int nodeCount = 1_000_000;

        var bitSet = HugeAtomicPagedBitSet.create(0);

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
