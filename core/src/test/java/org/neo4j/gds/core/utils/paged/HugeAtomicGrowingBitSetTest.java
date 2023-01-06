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

class HugeAtomicGrowingBitSetTest {

    @Test
    void testGrowEmpty() {
        var atomicBitSet = HugeAtomicGrowingBitSet.create(0);
        atomicBitSet.set(0);
        assertThat(atomicBitSet.get(0)).isTrue();
    }

    @Test
    void testGrow() {
        var atomicBitSet = HugeAtomicGrowingBitSet.create(42);

        assertThat(atomicBitSet.size()).isEqualTo(42);
        atomicBitSet.set(42);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(48);

        atomicBitSet.set(1337);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(1505);

        atomicBitSet.set(42_1337);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(474_005);

        atomicBitSet.set(42_1337_1337L);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337_1337L)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(4_740_042_755L);
    }

    @Test
    void writingAndGrowingShouldBeThreadSafe() {
        int concurrency = 8;
        int nodeCount = 1_000_000;

        var bitSet = HugeAtomicGrowingBitSet.create(0);

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
