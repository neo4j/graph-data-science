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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Arrays;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

abstract class ShardedLongLongMapTest {

    @Provide
    Arbitrary<long[]> ids() {
        return Arbitraries
            .longs()
            .greaterOrEqual(0)
            .array(long[].class)
            .ofMinSize(42)
            .uniqueElements();
    }

    @Provide
    Arbitrary<long[]> fixedSizeIds() {
        return Arbitraries
            .longs()
            .greaterOrEqual(0)
            .array(long[].class)
            .ofMinSize(10_000)
            .ofMaxSize(10_000)
            .uniqueElements();
    }

    @Provide
    Arbitrary<long[]> largeIds() {
        return Arbitraries
            .longs()
            .greaterOrEqual(Long.MAX_VALUE - 100_000)
            .array(long[].class)
            .ofMinSize(42)
            .uniqueElements();
    }

    @Property
    void testBidirectionalMapping(@ForAll("ids") long[] originalIds) {
        var builder = builder(1);
        builder.addNodes(originalIds);
        var map = builder.build();
        for (var originalId : originalIds) {
            assertThat(map.toOriginalNodeId(map.toMappedNodeId(originalId))).isEqualTo(originalId);
        }
    }

    @Test
    void testAddingSingleNode() {
        var builder = builder(1);
        builder.addNodes(42);
        var map = builder.build();
        assertThat(map.toMappedNodeId(42)).isEqualTo(0L);
    }

    @Property
    void testContains(@ForAll("ids") long[] originalIds) {
        var builder = builder(1);
        builder.addNodes(originalIds);
        var map = builder.build();

        for (var originalId : originalIds) {
            assertThat(map.contains(originalId)).isTrue();
        }

        // Create some random ids that aren't originalIds and
        // check that they are not contained in the map.
        Arrays.sort(originalIds);
        var rng = new SplittableRandom();
        LongStream
            .range(0, originalIds.length)
            .map(__ -> rng.nextLong(Long.MAX_VALUE))
            .filter(id -> Arrays.binarySearch(originalIds, id) < 0)
            .forEach(id -> assertThat(map.contains(id)).isFalse());
    }

    @Property
    void testAddNode(@ForAll("ids") long[] originalIds) {
        var builder = builder(1);
        for (int i = 0; i < originalIds.length; i++) {
            long originalId = originalIds[i];
            long mappedId = builder.addNode(originalId);
            assertThat(mappedId).isEqualTo(i);
        }
    }

    @Property
    void testMaxOriginalId(@ForAll("ids") long[] originalIds) {
        var builder = builder(1);
        builder.addNodes(originalIds);
        var map = builder.build();
        Arrays.sort(originalIds);
        var expectedMaxOriginalId = originalIds[originalIds.length - 1];
        assertThat(map.maxOriginalId()).isEqualTo(expectedMaxOriginalId);
    }

    @Property
    void testAddingMultipleNodes(@ForAll("ids") long[] originalIds) {
        var builder = builder(1);
        for (var originalId : originalIds) {
            builder.addNodes(originalId);
        }
        var map = builder.build();
        var mappedIds = new long[originalIds.length];
        for (int i = 0; i < map.size(); i++) {
            mappedIds[i] = map.toMappedNodeId(originalIds[i]);
        }
        assertThat(mappedIds).doesNotHaveDuplicates();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1024, 4096, 5000, 9999})
    void testSize(int expectedSize) {
        var builder = builder(1);
        for (int i = 0; i < expectedSize; i++) {
            builder.addNodes(i);
        }
        assertThat(builder.build().size()).isEqualTo(expectedSize);
    }

    @Property
    void testAddingUltraLargeOriginalIds(@ForAll("largeIds") long[] originalIds) {
        var builder = builder(1);
        builder.addNodes(originalIds);
        var map = builder.build();

        assertThat(map.size()).isEqualTo(originalIds.length);
        long[] mappedIds = new long[originalIds.length];
        for (int i = 0; i < map.size(); i++) {
            mappedIds[i] = map.toMappedNodeId(originalIds[i]);
        }
        assertThat(mappedIds).doesNotHaveDuplicates();
    }

    @Property(tries = 1)
    void testAddingMultipleNodesInParallel(@ForAll("fixedSizeIds") long[] originalIds) {
        int concurrency = 4;
        var builder = builder(concurrency);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            originalIds.length,
            partition -> (Runnable) () -> {
                long[] batch = new long[(int) partition.nodeCount()];
                System.arraycopy(originalIds, (int) partition.startNode(), batch, 0, batch.length);
                builder.addNodes(batch);
            },
            Optional.of(100)
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var map = builder.build();

        assertThat(map.size()).isEqualTo(originalIds.length);
        long[] mappedIds = new long[originalIds.length];
        for (int i = 0; i < map.size(); i++) {
            mappedIds[i] = map.toMappedNodeId(originalIds[i]);
        }
        assertThat(mappedIds).doesNotHaveDuplicates();
    }

    abstract TestBuilder builder(int concurrency);

    interface TestBuilder {
        long addNode(long nodeId);

        void addNodes(long... nodeIds);

        ShardedLongLongMap build();
    }

    static class DefaultBuilderTest extends ShardedLongLongMapTest {

        @Property
        void testAddNodeWithDuplicates(@ForAll("ids") long[] originalIds) {
            var builder = builder(1);
            for (long originalId : originalIds) {
                long mappedId = builder.addNode(originalId);
                long duplicateMappedId = builder.addNode(originalId);
                assertThat(duplicateMappedId).isEqualTo(-mappedId - 1);
            }
        }

        @Override
        TestBuilder builder(int concurrency) {
            return new DefaultBuilder(concurrency);
        }

        private static final class DefaultBuilder implements TestBuilder {
            private final ShardedLongLongMap.Builder inner;

            DefaultBuilder(int concurrency) {
                this.inner = ShardedLongLongMap.builder(concurrency);
            }

            @Override
            public long addNode(long nodeId) {
                return inner.addNode(nodeId);
            }

            @Override
            public void addNodes(long... nodeIds) {
                for (long nodeId : nodeIds) {
                    inner.addNode(nodeId);
                }
            }

            @Override
            public ShardedLongLongMap build() {
                return inner.build();
            }
        }
    }

    static final class BatchedBuilderTest extends ShardedLongLongMapTest {

        @Property
        void testAddingMultipleNodesInBatch(@ForAll("ids") long[] originalIds) {
            var builder = builder(1);
            builder.addNodes(originalIds);
            var map = builder.build();
            var mappedIds = new long[originalIds.length];
            for (int i = 0; i < map.size(); i++) {
                mappedIds[i] = map.toMappedNodeId(originalIds[i]);
            }
            assertThat(mappedIds).doesNotHaveDuplicates();
        }

        @Override
        TestBuilder builder(int concurrency) {
            return new BatchedBuilder(concurrency);
        }

        private static final class BatchedBuilder implements TestBuilder {
            private final ShardedLongLongMap.BatchedBuilder inner;

            BatchedBuilder(int concurrency) {
                inner = ShardedLongLongMap.batchedBuilder(concurrency);
            }

            @Override
            public long addNode(long nodeId) {
                var batch = inner.prepareBatch(1);
                return batch.addNode(nodeId);
            }

            @Override
            public void addNodes(long... nodeIds) {
                var batch = inner.prepareBatch(nodeIds.length);
                for (long nodeId : nodeIds) {
                    batch.addNode(nodeId);
                }
            }

            @Override
            public ShardedLongLongMap build() {
                return inner.build();
            }
        }
    }
}
