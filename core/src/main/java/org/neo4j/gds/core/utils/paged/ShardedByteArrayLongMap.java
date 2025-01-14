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

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.procedures.ObjectLongProcedure;
import org.eclipse.collections.impl.collection.mutable.AbstractMultiReaderMutableCollection;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.BitUtil;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public final class ShardedByteArrayLongMap {

    private final HugeObjectArray<byte[]> internalNodeMapping;
    private final ObjectLongMap<byte[]>[] originalNodeMappingShards;
    private final int shardShift;

    public static Builder builder(Concurrency concurrency) {
        return new Builder(concurrency);
    }

    public static Builder builder(Concurrency concurrency, long capacity) {
        return new Builder(concurrency, capacity);
    }

    private ShardedByteArrayLongMap(
        HugeObjectArray<byte[]> internalNodeMapping,
        ObjectLongMap<byte[]>[] originalNodeMappingShards,
        int shardShift
    ) {
        this.internalNodeMapping = internalNodeMapping;
        this.originalNodeMappingShards = originalNodeMappingShards;
        this.shardShift = shardShift;
    }

    public long toMappedNodeId(byte[] nodeId) {
        var shard = findShard(nodeId, this.originalNodeMappingShards, this.shardShift);
        return shard.getOrDefault(nodeId, IdMap.NOT_FOUND);
    }

    public boolean contains(byte[] originalId) {
        var shard = findShard(originalId, this.originalNodeMappingShards, this.shardShift);
        return shard.containsKey(originalId);
    }

    public byte[] toOriginalNodeId(long nodeId) {
        return internalNodeMapping.get(nodeId);
    }

    public long size() {
        return internalNodeMapping.size();
    }

    private static <T> T findShard(byte[] key, T[] shards, int shift) {
        int idx = shardIdx(key, shift);
        return shards[idx];
    }

    // https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
    private static final long FNV1_64_INIT = 0xcbf29ce484222325L;
    private static final long FNV1_64_PRIME = 1099511628211L;

    // We use FNV-1a 64-bit hash function to hash the byte array key
    // and achieve a somewhat uniform distribution of keys across shards.
    private static int shardIdx(byte[] key, int shift) {
        long hash = FNV1_64_INIT;

        for (int i = 0; i < key.length; i++) {
            hash ^= (key[i] & 0xff);
            hash *= FNV1_64_PRIME;
        }

        return (int) (hash >>> shift);
    }

    private static int numberOfShards(Concurrency concurrency) {
        return BitUtil.nextHighestPowerOfTwo(concurrency.value() * 4);
    }

    @SuppressWarnings("unchecked")
    private static <S extends MapShard> ShardedByteArrayLongMap build(
        long nodeCount,
        S[] shards,
        int shardShift
    ) {
        var internalNodeMapping = HugeObjectArray.newArray(byte[].class, nodeCount);
        var mapShards = new ObjectLongMap[shards.length];

        // ignoring concurrency limitation 🤷
        Arrays.parallelSetAll(
            mapShards, idx -> {
                var shard = shards[idx];
                var mapping = shard.intoMapping();
                mapping.forEach((ObjectLongProcedure<? super byte[]>) (originalId, mappedId) -> {
                    internalNodeMapping.set(mappedId, originalId);
                });
                return mapping;
            }
        );

        return new ShardedByteArrayLongMap(
            internalNodeMapping,
            mapShards,
            shardShift
        );
    }

    abstract static class MapShard {

        private static final class Map extends ObjectLongHashMap<byte[]> {

            Map() {
                super();
            }

            Map(int initialCapacity) {
                super(initialCapacity);
            }

            @Override
            protected int hashKey(byte[] key) {
                return BitMixer.mix(Arrays.hashCode(key), this.keyMixer);
            }

            @Override
            protected boolean equals(Object v1, Object v2) {
                return Arrays.equals((byte[]) v1, (byte[]) v2);
            }
        }

        private final ReentrantLock lock;
        private final AbstractMultiReaderMutableCollection.LockWrapper lockWrapper;
        final ObjectLongMap<byte[]> mapping;

        MapShard() {
            this.mapping = new Map();
            this.lock = new ReentrantLock();
            this.lockWrapper = new AbstractMultiReaderMutableCollection.LockWrapper(lock);
        }

        MapShard(long capacity) {
            this.mapping = new Map(Math.toIntExact(capacity));
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

        ObjectLongMap<byte[]> intoMapping() {
            return mapping;
        }
    }

    public static final class Builder {

        private final AtomicLong nodeCount;
        private final Shard[] shards;
        private final int shardShift;
        private final int shardMask;

        Builder(Concurrency concurrency) {
            this.nodeCount = new AtomicLong();
            int numberOfShards = numberOfShards(concurrency);
            this.shardShift = Long.SIZE - Integer.numberOfTrailingZeros(numberOfShards);
            this.shardMask = numberOfShards - 1;
            this.shards = IntStream.range(0, numberOfShards)
                .mapToObj(__ -> new Shard(this.nodeCount))
                .toArray(Shard[]::new);
        }

        Builder(Concurrency concurrency, long capacity) {
            this.nodeCount = new AtomicLong();
            int numberOfShards = numberOfShards(concurrency);
            this.shardShift = Long.SIZE - Integer.numberOfTrailingZeros(numberOfShards);
            this.shardMask = numberOfShards - 1;
            this.shards = IntStream.range(0, numberOfShards)
                .mapToObj(__ -> new Shard(this.nodeCount, BitUtil.ceilDiv(capacity, numberOfShards)))
                .toArray(Shard[]::new);
        }

        /**
         * Add a node to the mapping.
         *
         * @return {@code mappedId >= 0} if the node was added,
         *     or {@code -(mappedId) - 1} if the node was already mapped.
         */
        public long addNode(byte[] nodeId) {
            var shard = findShard(nodeId, this.shards, this.shardShift);
            try (var ignoredLock = shard.acquireLock()) {
                return shard.addNode(nodeId);
            }
        }

        public ShardedByteArrayLongMap build() {
            return ShardedByteArrayLongMap.build(
                this.nodeCount.get(),
                this.shards,
                this.shardShift
            );
        }

        private static final class Shard extends MapShard {
            private final AtomicLong nextId;

            private Shard(AtomicLong nextId) {
                super();
                this.nextId = nextId;
            }

            private Shard(AtomicLong nextId, long capacity) {
                super(capacity);
                this.nextId = nextId;
            }

            long addNode(byte[] nodeId) {
                this.assertIsUnderLock();
                int index = mapping.indexOf(nodeId);
                if (mapping.indexExists(index)) {
                    return -mapping.indexGet(index) - 1;
                }
                long mappedId = nextId.getAndIncrement();
                mapping.indexInsert(index, nodeId, mappedId);
                return mappedId;
            }
        }
    }
}
