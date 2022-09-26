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

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.SpreadFunctions;
import org.eclipse.collections.impl.collection.mutable.AbstractMultiReaderMutableCollection;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.IdMapAllocator;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public final class ShardedLongLongMap {

    private final HugeLongArray internalNodeMapping;
    private final LongLongMap[] originalNodeMappingShards;
    private final int shardShift;
    private final int shardMask;
    private final long maxOriginalId;

    public static Builder builder(int concurrency) {
        return new Builder(concurrency);
    }

    public static BatchedBuilder batchedBuilder(int concurrency) {
        return new BatchedBuilder(concurrency);
    }

    private ShardedLongLongMap(
        HugeLongArray internalNodeMapping,
        LongLongMap[] originalNodeMappingShards,
        int shardShift,
        int shardMask,
        long maxOriginalId
    ) {
        this.internalNodeMapping = internalNodeMapping;
        this.originalNodeMappingShards = originalNodeMappingShards;
        this.shardShift = shardShift;
        this.shardMask = shardMask;
        this.maxOriginalId = maxOriginalId;
    }

    public long toMappedNodeId(long nodeId) {
        var shard = findShard(nodeId, this.originalNodeMappingShards, this.shardShift, this.shardMask);
        return shard.getIfAbsent(nodeId, -1L);
    }

    public boolean contains(long originalId) {
        var shard = findShard(originalId, this.originalNodeMappingShards, this.shardShift, this.shardMask);
        return shard.containsKey(originalId);
    }

    public long toOriginalNodeId(long nodeId) {
        return internalNodeMapping.get(nodeId);
    }

    public long maxOriginalId() {
        return maxOriginalId;
    }

    public long size() {
        return internalNodeMapping.size();
    }

    private static <T> T findShard(long key, T[] shards, int shift, int mask) {
        int idx = shardIdx2(key, shift, mask);
        return shards[idx];
    }

    // plain "key % numShards" - fastest but might have poor distribution depending on the key
    private static int shardIdx(long key, int shift, int mask) {
        return (int) (key % mask);
    }

    // use a hash function to try to get a uniform distribution independent of the key
    private static int shardIdx2(long key, int shift, int mask) {
        long hash = SpreadFunctions.longSpreadOne(key);
        return (int) (hash >>> shift);
    }

    private static int numberOfShards(int concurrency) {
        return BitUtil.nextHighestPowerOfTwo(concurrency * 4);
    }

    private static <S extends MapShard> ShardedLongLongMap build(
        long nodeCount,
        S[] shards,
        int shardShift,
        int shardMask
    ) {
        var internalNodeMapping = HugeLongArray.newArray(nodeCount);
        var mapShards = new LongLongMap[shards.length];
        var maxOriginalIds = new long[shards.length];

        // ignoring concurrency limitation 🤷
        Arrays.parallelSetAll(mapShards, idx -> {
            var maxOriginalId = new MutableLong(0);
            var shard = shards[idx];
            var mapping = shard.intoMapping();
            mapping.forEachKeyValue((originalId, mappedId) -> {
                if (originalId > maxOriginalId.longValue()) {
                    maxOriginalId.setValue(originalId);
                }
                internalNodeMapping.set(mappedId, originalId);
            });
            maxOriginalIds[idx] = maxOriginalId.longValue();
            return mapping;
        });

        return new ShardedLongLongMap(
            internalNodeMapping,
            mapShards,
            shardShift,
            shardMask,
            Arrays.stream(maxOriginalIds).max().orElse(0)
        );
    }

    private static <S extends MapShard> ShardedLongLongMap build(
        long nodeCount,
        S[] shards,
        int shardShift,
        int shardMask,
        long maxOriginalId
    ) {
        var internalNodeMapping = HugeLongArray.newArray(nodeCount);
        var mapShards = new LongLongMap[shards.length];

        // ignoring concurrency limitation 🤷
        Arrays.parallelSetAll(mapShards, idx -> {
            var shard = shards[idx];
            var mapping = shard.intoMapping();
            mapping.forEachKeyValue((originalId, mappedId) -> {
                internalNodeMapping.set(mappedId, originalId);
            });
            return mapping;
        });

        return new ShardedLongLongMap(
            internalNodeMapping,
            mapShards,
            shardShift,
            shardMask,
            maxOriginalId
        );
    }

    abstract static class MapShard {

        private final ReentrantLock lock;
        private final AbstractMultiReaderMutableCollection.LockWrapper lockWrapper;
        final MutableLongLongMap mapping;

        MapShard() {
            this.mapping = LongLongMaps.mutable.empty();
            this.lock = new ReentrantLock();
            this.lockWrapper = new AbstractMultiReaderMutableCollection.LockWrapper(lock);
        }

        final AbstractMultiReaderMutableCollection.LockWrapper acquireLock() {
            this.lock.lock();
            return this.lockWrapper;
        }

        void assertIsUnderLock() {
            assert this.lock.isHeldByCurrentThread() : "addNode must only be called while holding the lock";
        }

        MutableLongLongMap intoMapping() {
            return mapping;
        }
    }

    public static final class Builder {

        private final AtomicLong nodeCount;
        private final Shard[] shards;
        private final int shardShift;
        private final int shardMask;

        Builder(int concurrency) {
            this.nodeCount = new AtomicLong();
            int numberOfShards = numberOfShards(concurrency);
            this.shardShift = Long.SIZE - Integer.numberOfTrailingZeros(numberOfShards);
            this.shardMask = numberOfShards - 1;
            this.shards = IntStream.range(0, numberOfShards)
                .mapToObj(__ -> new Shard(this.nodeCount))
                .toArray(Shard[]::new);
        }

        public long addNode(long nodeId) {
            var shard = findShard(nodeId, this.shards, this.shardShift, this.shardMask);
            try (var ignoredLock = shard.acquireLock()) {
                return shard.addNode(nodeId);
            }
        }

        public long toMappedNodeId(long nodeId) {
            var shard = findShard(nodeId, this.shards, this.shardShift, this.shardMask);
            return shard.toMappedNodeId(nodeId);
        }

        public ShardedLongLongMap build() {
            return ShardedLongLongMap.build(
                this.nodeCount.get(),
                this.shards,
                this.shardShift,
                this.shardMask
            );
        }

        public ShardedLongLongMap build(long maxOriginalId) {
            return ShardedLongLongMap.build(
                this.nodeCount.get(),
                this.shards,
                this.shardShift,
                this.shardMask,
                maxOriginalId
            );
        }

        private static final class Shard extends MapShard {
            private final AtomicLong nextId;

            private Shard(AtomicLong nextId) {
                super();
                this.nextId = nextId;
            }

            long toMappedNodeId(long nodeId) {
                return mapping.getIfAbsent(nodeId, IdMap.NOT_FOUND);
            }

            long addNode(long nodeId) {
                this.assertIsUnderLock();
                long internalId = this.nextId.getAndIncrement();
                mapping.put(nodeId, internalId);
                return internalId;
            }
        }
    }

    public static final class BatchedBuilder {

        private final AtomicLong nodeCount;
        private final Shard[] shards;
        private final CloseableThreadLocal<Batch> batches;
        private final int shardShift;
        private final int shardMask;

        BatchedBuilder(int concurrency) {
            this.nodeCount = new AtomicLong();
            int numberOfShards = numberOfShards(concurrency);
            this.shardShift = Long.SIZE - Integer.numberOfTrailingZeros(numberOfShards);
            this.shardMask = numberOfShards - 1;
            this.shards = IntStream.range(0, numberOfShards)
                .mapToObj(__ -> new Shard())
                .toArray(Shard[]::new);
            this.batches = CloseableThreadLocal.withInitial(() -> new Batch(
                this.shards,
                this.shardShift,
                this.shardMask
            ));
        }

        public Batch prepareBatch(int nodeCount) {
            var startId = this.nodeCount.getAndAdd(nodeCount);
            var batch = this.batches.get();
            batch.initBatch(startId, nodeCount);
            return batch;
        }

        public ShardedLongLongMap build() {
            this.batches.close();
            return ShardedLongLongMap.build(
                this.nodeCount.get(),
                this.shards,
                this.shardShift,
                this.shardMask
            );
        }

        public ShardedLongLongMap build(long maxOriginalId) {
            this.batches.close();
            return ShardedLongLongMap.build(
                this.nodeCount.get(),
                this.shards,
                this.shardShift,
                this.shardMask,
                maxOriginalId
            );
        }

        public static final class Batch implements IdMapAllocator {

            private final Shard[] shards;
            private final int shardShift;
            private final int shardMask;

            private long startId;
            private int length;

            private Batch(Shard[] shards, int shardShift, int shardMask) {
                this.shards = shards;
                this.shardShift = shardShift;
                this.shardMask = shardMask;
            }

            @Override
            public int allocatedSize() {
                return this.length;
            }

            @Override
            public void insert(long[] nodeIds) {
                int length = allocatedSize();
                for (int i = 0; i < length; i++) {
                    addNode(nodeIds[i]);
                }
            }

            public long addNode(long nodeId) {
                long mappedId = this.startId++;
                var shard = findShard(nodeId, this.shards, this.shardShift, this.shardMask);
                try (var ignoredLock = shard.acquireLock()) {
                    shard.addNode(nodeId, mappedId);
                }
                return mappedId;
            }

            void initBatch(long startId, int length) {
                this.startId = startId;
                this.length = length;
            }
        }

        private static final class Shard extends MapShard {

            void addNode(long nodeId, long mappedId) {
                this.assertIsUnderLock();
                this.mapping.put(nodeId, mappedId);
            }
        }
    }

}
