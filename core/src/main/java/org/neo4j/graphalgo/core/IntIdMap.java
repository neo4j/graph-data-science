/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.OpenHashContainers;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.function.LongPredicate;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public class IntIdMap {
    private static final MemoryEstimation ESTIMATION = MemoryEstimations
            .builder(IntIdMap.class)
            .field("iter", IdIterator.class)
            .perNode("graphIds", nodeCount -> sizeOfLongArray((int) nodeCount))
            .startField("nodeToGraphIds", LongIntHashMap.class)
            .perNode("buffers", nodeCount -> {
                int bufferSize = OpenHashContainers.expectedBufferSize((int) nodeCount);
                return sizeOfLongArray(bufferSize) +
                       sizeOfIntArray(bufferSize);
            })
            .endField()
            .build();

    private final IdIterator iter;
    private int nextGraphId;
    private long[] graphIds;
    private final LongIntHashMap nodeToGraphIds;

    /**
     * initialize the map with maximum node capacity
     */
    IntIdMap(final int capacity) {
        nodeToGraphIds = new LongIntHashMap(capacity);
        iter = new IdIterator();
    }

    public static MemoryEstimation memoryEstimation() {
        return ESTIMATION;
    }

    public PrimitiveLongIterator iterator() {
        final PrimitiveIntIterator base = iter.reset(nextGraphId);
        return new PrimitiveLongIterator() {
            @Override
            public boolean hasNext() {
                return base.hasNext();
            }

            @Override
            public long next() {
                return base.next();
            }
        };
    }

    private int mapOrGet(long longValue) {
        int intValue = nodeToGraphIds.getOrDefault(longValue, -1);
        if (intValue == -1) {
            intValue = nextGraphId++;
            nodeToGraphIds.put(longValue, intValue);
        }
        return intValue;
    }

    public int add(long longValue) {
        int intValue = nextGraphId++;
        nodeToGraphIds.put(longValue, intValue);
        return intValue;
    }

    public int get(long longValue) {
        return nodeToGraphIds.getOrDefault(longValue, -1);
    }

    void buildMappedIds(AllocationTracker tracker) {
        tracker.add(sizeOfInstance(IntIdMap.class));
        tracker.add(sizeOfLongArray(nodeToGraphIds.keys.length));
        tracker.add(sizeOfIntArray(nodeToGraphIds.values.length));
        tracker.add(sizeOfLongArray(size()));
        graphIds = new long[size()];
        for (final LongIntCursor cursor : nodeToGraphIds) {
            graphIds[cursor.value] = cursor.key;
        }
    }

    public int size() {
        return nextGraphId;
    }

    public void forEachNode(LongPredicate consumer) {
        int limit = this.nextGraphId;
        for (int i = 0; i < limit; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    public int toMappedNodeId(long nodeId) {
        return mapOrGet(nodeId);
    }

    public long toOriginalNodeId(long nodeId) {
        return graphIds[(int) nodeId];
    }

    public boolean contains(final long nodeId) {
        return nodeToGraphIds.containsKey(nodeId);
    }

    public long nodeCount() {
        return graphIds.length;
    }

    private static final class IdIterator implements PrimitiveIntIterator {

        private int current;
        private int limit; // exclusive upper bound

        private PrimitiveIntIterator reset(int length) {
            current = 0;
            limit = length;
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
