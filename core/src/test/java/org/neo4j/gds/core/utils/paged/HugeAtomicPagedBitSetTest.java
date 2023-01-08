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
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.utils.paged.HugeAtomicPagedBitSet.PAGE_SHIFT_BITS;

class HugeAtomicPagedBitSetTest {

    @Test
    void testSinglePageBitSet() {
        var atomicBitSet = HugeAtomicPagedBitSet.create(42);
        assertThat(atomicBitSet.get(23)).isFalse();
        atomicBitSet.set(23);
        assertThat(atomicBitSet.get(23)).isTrue();
    }

    @Test
    void testMultiPageBitSet() {
        long size = 2 * (1L << PAGE_SHIFT_BITS) + 42; // 3 pages

        var atomicBitSet = HugeAtomicPagedBitSet.create(size);
        // page 0
        assertThat(atomicBitSet.get(23)).isFalse();
        atomicBitSet.set(23);
        assertThat(atomicBitSet.get(23)).isTrue();
        // page 1
        assertThat(atomicBitSet.get((1L << PAGE_SHIFT_BITS) + 23)).isFalse();
        atomicBitSet.set((1L << PAGE_SHIFT_BITS) + 23);
        assertThat(atomicBitSet.get((1L << PAGE_SHIFT_BITS) + 23)).isTrue();
        // page 2
        assertThat(atomicBitSet.get(2 * (1L << PAGE_SHIFT_BITS) + 23)).isFalse();
        atomicBitSet.set(2 * (1L << PAGE_SHIFT_BITS) + 23);
        assertThat(atomicBitSet.get(2 * (1L << PAGE_SHIFT_BITS) + 23)).isTrue();
    }

    @Test
    void testCardinality() {
        long size = 2 * (1L << PAGE_SHIFT_BITS) + 42; // 3 pages

        var atomicBitSet = HugeAtomicPagedBitSet.create(size);
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
