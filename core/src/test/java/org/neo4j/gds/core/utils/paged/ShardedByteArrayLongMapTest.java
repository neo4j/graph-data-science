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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Size;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedByteArrayLongMapTest {

    @Provide
    Arbitrary<byte[][]> nodes() {
        var idGen = Arbitraries.bytes().array(byte[].class).ofSize(10);
        return Arbitraries
            .create(idGen::sample)
            .array(byte[][].class);
    }

    @Test
    void addSingleNode() {
        byte[] node = "foobar".getBytes(StandardCharsets.UTF_8);
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        long mapped = builder.addNode(node);
        assertThat(mapped).isGreaterThanOrEqualTo(0);
        var map = builder.build();
        assertThat(map.toMappedNodeId(node)).isEqualTo(mapped);
        assertThat(map.toOriginalNodeId(mapped)).isEqualTo(node);
    }

    @Property
    void addNodes(@ForAll("nodes") @Size(100) byte[][] nodes) {
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        var size = 0;
        for (byte[] node : nodes) {
            if (builder.addNode(node) >= 0) {
                size++;
            }

        }
        var map = builder.build();

        assertThat(map.size()).isEqualTo(size);
        for (byte[] node : nodes) {
            assertThat(map.toOriginalNodeId(map.toMappedNodeId(node))).isEqualTo(node);
        }
    }

    @Property
    void addNodesDifferentObject(@ForAll("nodes") @Size(100) byte[][] nodes) {
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        var size = 0;
        for (byte[] node : nodes) {
            if (builder.addNode(node) >= 0) {
                size++;
            }
        }
        var map = builder.build();

        assertThat(map.size()).isEqualTo(size);
        for (byte[] node : nodes) {
            // Ensure that hashCode and equals work correctly for byte arrays
            // with same elements, but different objects.
            var nodeCopy = Arrays.copyOf(node, node.length);
            assertThat(map.toOriginalNodeId(map.toMappedNodeId(nodeCopy))).isEqualTo(node);
        }
    }

    @Test
    void addExistingNode() {
        byte[] node1 = "foobar".getBytes(StandardCharsets.UTF_8);
        byte[] node2 = "foobar".getBytes(StandardCharsets.UTF_8);
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        long mappedNode1 = builder.addNode(node1);
        assertThat(mappedNode1).isGreaterThanOrEqualTo(0);
        long mappedNode2 = builder.addNode(node2);
        assertThat(mappedNode2).isEqualTo(-(mappedNode1 + 1));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1024, 4096, 5000, 9999})
    void size(int expectedSize) {
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        for (int i = 0; i < expectedSize; i++) {
            builder.addNode(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        assertThat(builder.build().size()).isEqualTo(expectedSize);
    }

    @Test
    void toMappedNodeId() {
        byte[] node = "foobar".getBytes(StandardCharsets.UTF_8);
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        long mapped = builder.addNode(node);
        var map = builder.build();
        assertThat(map.toMappedNodeId(node)).isEqualTo(mapped);
    }

    @Test
    void toOriginalNodeId() {
        byte[] node = "foobar".getBytes(StandardCharsets.UTF_8);
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        long mapped = builder.addNode(node);
        var map = builder.build();
        assertThat(map.toOriginalNodeId(mapped)).isEqualTo(node);
    }

    @Test
    void contains() {
        byte[] node = "foobar".getBytes(StandardCharsets.UTF_8);
        var builder = ShardedByteArrayLongMap.builder(new Concurrency(1));
        builder.addNode(node);
        var map = builder.build();
        assertThat(map.contains(node)).isTrue();
        assertThat(map.contains("barfoo".getBytes(StandardCharsets.UTF_8))).isFalse();
    }

    @Property(tries = 1)
    void testAddingMultipleNodesInParallel(@ForAll("nodes") @Size(10000) byte[][] originalIds) {
        var concurrency = new Concurrency(4);
        var builder = ShardedByteArrayLongMap.builder(concurrency);
        var size = new AtomicInteger(0);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            originalIds.length,
            partition -> (Runnable) () -> {
                byte[][] batch = new byte[(int) partition.nodeCount()][];
                System.arraycopy(originalIds, (int) partition.startNode(), batch, 0, batch.length);
                for (byte[] node : batch) {
                    if (builder.addNode(node) >= 0) {
                        size.incrementAndGet();
                    }
                }
            },
            Optional.of(100)
        );

        ParallelUtil.run(tasks, DefaultPool.INSTANCE);

        var map = builder.build();

        assertThat(map.size()).isEqualTo(size.get());
        for (byte[] node : originalIds) {
            assertThat(map.toOriginalNodeId(map.toMappedNodeId(node))).isEqualTo(node);
        }
    }
}
