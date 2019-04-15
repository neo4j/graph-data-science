/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.MemoryUsage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.IntPredicate;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public final class IdMap implements IdMapping, NodeIterator, BatchNodeIterable {

    private final IdIterator iter;
    private int nextGraphId;
    private long[] graphIds;
    private LongIntHashMap nodeToGraphIds;

    /**
     * initialize the map with maximum node capacity
     */
    public IdMap(final int capacity) {
        nodeToGraphIds = new LongIntHashMap(capacity);
        iter = new IdIterator();
    }

    /**
     * CTor used by deserializing logic
     */
    public IdMap(
            long[] graphIds,
            LongIntHashMap nodeToGraphIds) {
        this.nextGraphId = graphIds.length;
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        iter = new IdIterator();
    }

    public PrimitiveIntIterator iterator() {
        return iter.reset(nextGraphId);
    }

    public int mapOrGet(long longValue) {
        int intValue = nodeToGraphIds.getOrDefault(longValue, -1);
        if (intValue == -1) {
            intValue = nextGraphId++;
            nodeToGraphIds.put(longValue, intValue);
        }
        return intValue;
    }

    public void add(long longValue) {
        int intValue = nextGraphId++;
        nodeToGraphIds.put(longValue, intValue);
    }

    public int get(long longValue) {
        return nodeToGraphIds.getOrDefault(longValue, -1);
    }

    public void buildMappedIds(AllocationTracker tracker) {
        tracker.add(MemoryUsage.shallowSizeOfInstance(IdMap.class));
        tracker.add(MemoryUsage.sizeOfLongArray(nodeToGraphIds.keys.length));
        tracker.add(MemoryUsage.sizeOfIntArray(nodeToGraphIds.values.length));
        tracker.add(MemoryUsage.sizeOfLongArray(size()));
        graphIds = new long[size()];
        for (final LongIntCursor cursor : nodeToGraphIds) {
            graphIds[cursor.value] = cursor.key;
        }
    }

    public int size() {
        return nextGraphId;
    }

    public long[] mappedIds() {
        return graphIds;
    }

    public LongIntMap nodeToGraphIds() {
        return nodeToGraphIds;
    }

    public void forEach(IntPredicate consumer) {
        int limit = this.nextGraphId;
        for (int i = 0; i < limit; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return mapOrGet(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return graphIds[nodeId];
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeToGraphIds.containsKey(nodeId);
    }

    @Override
    public long nodeCount() {
        return graphIds.length;
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        final int count = graphIds.length;
        for (int i = 0; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return new IdIterator().reset(graphIds.length);
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        int nodeCount = graphIds.length;
        int numberOfBatches = ParallelUtil.threadSize(batchSize, nodeCount);
        if (numberOfBatches == 1) {
            return Collections.singleton(this::nodeIterator);
        }
        PrimitiveIntIterable[] iterators = new PrimitiveIntIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> {
            int start = i * batchSize;
            int length = Math.min(batchSize, nodeCount - start);
            return new IdIterable(start, length);
        });
        return Arrays.asList(iterators);
    }

    public static final class IdIterable implements PrimitiveIntIterable {
        private final int start;
        private final int length;

        public IdIterable(int start, int length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return new IdIterator().reset(start, length);
        }
    }

    private static final class IdIterator implements PrimitiveIntIterator {

        private int current;
        private int limit; // exclusive upper bound

        private PrimitiveIntIterator reset(int length) {
            return reset(0, length);
        }

        private PrimitiveIntIterator reset(int start, int length) {
            current = start;
            this.limit = start + length;
            return this;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public int next() {
            return current++;
        }
    }
}
