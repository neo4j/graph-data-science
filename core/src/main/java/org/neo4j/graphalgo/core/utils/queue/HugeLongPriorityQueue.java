/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeArrays;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfBitset;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

/**
 * A PriorityQueue specialized for longs that maintains a partial ordering of
 * its elements such that the smallest value can always be found in constant time.
 * The definition of what <i>small</i> means is up to the implementing subclass.
 * <p>
 * Put()'s and pop()'s require log(size) time but the remove() cost implemented here is linear.
 * <p>
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * Implementation has been copied from https://issues.apache.org/jira/browse/SOLR-2092
 * and slightly adapted to our needs.
 */
public abstract class HugeLongPriorityQueue implements PrimitiveLongIterable {

    public static MemoryEstimation memoryEstimation(long capacity) {
        return MemoryEstimations.builder(HugeLongPriorityQueue.class)
            .fixed("heap", sizeOfLongArray(capacity))
            .fixed("costs", sizeOfDoubleArray(capacity))
            .fixed("keys", sizeOfBitset(capacity))
            .build();
    }

    private HugeLongArray heap;
    protected HugeDoubleArray costs;
    protected BitSet keys;

    private long size = 0;

    /**
     * Creates a new queue with the given capacity.
     */
    HugeLongPriorityQueue(final long initialCapacity) {
        final long heapSize;
        if (0 == initialCapacity) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based.  heap[0] is unused.
            heapSize = initialCapacity + 1;
        }
        this.heap = HugeLongArray.newArray(heapSize, AllocationTracker.empty());
        this.costs = HugeDoubleArray.newArray(initialCapacity, AllocationTracker.empty());
        this.keys = new BitSet(initialCapacity);
    }

    /**
     * Defines the ordering of the queue.
     * Returns true iff {@code a} is strictly less than {@code b}.
     * <p>
     * The default behavior assumes a min queue, where the smallest value is on top.
     * To implement a max queue, return {@code b < a}.
     * The resulting order is not stable.
     */
    protected abstract boolean lessThan(long a, long b);

    protected boolean addCost(long element, double cost) {
        double oldCost = costs.get(element);
        boolean elementExists = keys.get(element);
        costs.set(element, cost);
        keys.set(element);
        return oldCost != cost || !elementExists;
    }

    protected void removeCost(final long element) {
        keys.clear(element);
    }

    protected double cost(long element) {
        if (keys.get(element)) {
            return costs.get(element);
        }
        return 0D;
    }

    /**
     * Adds an long associated with the given weight to a queue in log(size) time.
     * <p>
     * NOTE: The default implementation does nothing with the cost parameter.
     * It is up to the implementation how the cost parameter is used.
     */
    public final void add(long element, double cost) {
        addCost(element, cost);
        size++;
        ensureCapacityForInsert();
        heap.set(size, element);
        upHeap(size);
    }

    private void add(long element) {
        size++;
        ensureCapacityForInsert();
        heap.set(size, element);
        upHeap(size);
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public final long top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap.get(1);
    }

    /**
     * Removes and returns the least element of the queue in log(size) time.
     *
     * @return the least element of the queue in log(size) time while removing it.
     */
    public final long pop() {
        if (size > 0) {
            long result = heap.get(1);    // save first value
            heap.set(1, heap.get(size));    // move last to first
            size--;
            downHeap(1);           // adjust heap
            removeCost(result);
            return result;
        } else {
            return -1;
        }
    }

    /**
     * @return the number of elements currently stored in the queue.
     */
    public final long size() {
        return size;
    }

    /**
     * @return true iff there are currently no elements stored in the queue.
     */
    public final boolean isEmpty() {
        return size == 0;
    }

    /**
     * Removes all entries from the queue.
     */
    public void clear() {
        size = 0;
        keys.clear();
    }

    /**
     * Updates the heap because the cost of an element has changed, possibly from the outside.
     * Cost is linear with the size of the queue.
     */
    public final void update(int element) {
        long pos = findElementPosition(element);
        if (pos != 0) {
            if (!upHeap(pos) && pos < size) {
                downHeap(pos);
            }
        }
    }

    public final void set(int element, double cost) {
        if (addCost(element, cost)) {
            update(element);
        } else {
            add(element);
        }
    }

    private long findElementPosition(int element) {
        final long limit = size + 1;
        final HugeLongArray data = heap;
        HugeCursor<long[]> cursor = data.initCursor(data.newCursor(), 1, limit);
        while (cursor.next()) {
            long[] internalArray = cursor.array;
            int i = cursor.offset;
            int localLimit = cursor.limit - 4;
            for (; i <= localLimit; i += 4) {
                if (internalArray[i] == element) return i + cursor.base;
                if (internalArray[i + 1] == element) return i + 1 + cursor.base;
                if (internalArray[i + 2] == element) return i + 2 + cursor.base;
                if (internalArray[i + 3] == element) return i + 3 + cursor.base;
            }
            for (; i < cursor.limit; ++i) {
                if (internalArray[i] == element) return i + cursor.base;
            }
        }
        return 0;
    }

    /**
     * Removes all entries from the queue, releases all buffers.
     * The queue can no longer be used afterwards.
     */
    public void release() {
        size = 0;
        heap = null;
        keys = null;
        costs.release();
    }

    private boolean upHeap(long origPos) {
        long i = origPos;
        long node = heap.get(i);          // save bottom node
        long j = i >>> 1;
        while (j > 0 && lessThan(node, heap.get(j))) {
            heap.set(i, heap.get(j));       // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap.set(i, node);              // install saved node
        return i != origPos;
    }

    private void downHeap(long i) {
        long node = heap.get(i);          // save top node
        long j = i << 1;              // find smaller child
        long k = j + 1;
        if (k <= size && lessThan(heap.get(k), heap.get(j))) {
            j = k;
        }
        while (j <= size && lessThan(heap.get(j), node)) {
            heap.set(i, heap.get(j));       // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap.get(k), heap.get(j))) {
                j = k;
            }
        }
        heap.set(i, node);              // install saved node
    }

    private void ensureCapacityForInsert() {
        if (size >= heap.size()) {
            long oversize = HugeArrays.oversize(size + 1, Integer.BYTES);
            heap = heap.copyOf(oversize, AllocationTracker.empty());
        }
    }

    @Override
    public PrimitiveLongIterator iterator() {
        return new PrimitiveLongIterator() {

            int i = 1;

            @Override
            public boolean hasNext() {
                return i <= size;
            }

            /**
             * @throws ArrayIndexOutOfBoundsException when the iterator is exhausted.
             */
            @Override
            public long next() {
                return heap.get(i++);
            }
        };
    }

    public static HugeLongPriorityQueue min(int capacity) {
        return new HugeLongPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costs.get(a) < costs.get(b);
            }
        };
    }

    public static HugeLongPriorityQueue max(int capacity) {
        return new HugeLongPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costs.get(a) > costs.get(b);
            }
        };
    }

}
