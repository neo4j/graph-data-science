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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.SpreadFunctions;
import org.eclipse.collections.impl.collection.mutable.AbstractMultiReaderMutableCollection;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.gds.mem.BitUtil;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

public final class ShardedLongSet {

    private final MapShard[] shards;
    private final int shardShift;
    private final int shardMask;

    public static ShardedLongSet of(int concurrency) {
        int numberOfShards = numberOfShards(concurrency);
        var shardShift = Long.SIZE - Integer.numberOfTrailingZeros(numberOfShards);
        var shardMask = numberOfShards - 1;
        var shards = IntStream.range(0, numberOfShards)
            .mapToObj(__ -> new MapShard())
            .toArray(MapShard[]::new);

        return new ShardedLongSet(shards, shardShift, shardMask);
    }

    private ShardedLongSet(
        MapShard[] shards, int shardShift, int shardMask
    ) {
        this.shards = shards;
        this.shardShift = shardShift;
        this.shardMask = shardMask;
    }

    public boolean addNode(long nodeId) {
        var shard = findShard(nodeId, this.shards, this.shardShift, this.shardMask);
        try (var ignoredLock = shard.acquireLock()) {
            return shard.addNode(nodeId);
        }
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

    private static class MapShard {

        private final ReentrantLock lock;
        private final AbstractMultiReaderMutableCollection.LockWrapper lockWrapper;
        private final MutableLongSet mapping;

        MapShard() {
            this.mapping = LongSets.mutable.empty();
            this.lock = new ReentrantLock();
            this.lockWrapper = new AbstractMultiReaderMutableCollection.LockWrapper(lock);
        }

        private AbstractMultiReaderMutableCollection.LockWrapper acquireLock() {
            this.lock.lock();
            return this.lockWrapper;
        }

        private boolean addNode(long nodeId) {
            return mapping.add(nodeId);
        }
    }
}
