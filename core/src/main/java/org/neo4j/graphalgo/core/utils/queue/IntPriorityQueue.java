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
package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Arrays;

/**
 * A PriorityQueue specialized for ints that maintains a partial ordering of
 * its elements such that the smallest value can always be found in constant time.
 * The definition of what <i>small</i> means is up to the implementing subclass.
 * <p>
 * Put()'s and pop()'s require log(size) time but the remove() cost implemented here is linear.
 * <p>
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class IntPriorityQueue implements PrimitiveIntIterable {

    public static final int DEFAULT_CAPACITY = 14;

    private static final int[] EMPTY_INT = new int[0];

    private int[] heap;
    private int size = 0;

    /**
     * Creates a new queue with the given capacity.
     * The queue dynamically grows to hold all elements.
     */
    IntPriorityQueue(final int initialCapacity) {
        final int heapSize;
        if (0 == initialCapacity) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based.  heap[0] is unused.
            heapSize = initialCapacity + 1;
        }
        this.heap = new int[ArrayUtil.oversize(heapSize, Integer.BYTES)];
    }

    /**
     * Defines the ordering of the queue.
     * Returns true iff {@code a} is strictly less than {@code b}.
     * <p>
     * The default behavior assumes a min queue, where the smallest value is on top.
     * To implement a max queue, return {@code b < a}.
     * The resulting order is not stable.
     */
    protected abstract boolean lessThan(int a, int b);

    /**
     * Adds the cost for the given element.
     *
     * @return true if the cost was changed, indicating that the node is already in the queue.
     */
    protected abstract boolean addCost(int element, double cost);

    /**
     * remove given element from cost management
     */
    protected abstract void removeCost(int element);

    /**
     * Gets the cost for the given element.
     */
    protected abstract double cost(int element);

    /**
     * Adds an int associated with the given weight to a queue in log(size) time.
     * <p>
     * NOTE: The default implementation does nothing with the cost parameter.
     * It is up to the implementation how the cost parameter is used.
     */
    public final void add(int element, double cost) {
        addCost(element, cost);
        size++;
        ensureCapacityForInsert();
        heap[size] = element;
        upHeap(size);
    }

    private void add(int element) {
        size++;
        ensureCapacityForInsert();
        heap[size] = element;
        upHeap(size);
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public final int top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap[1];
    }

    /**
     * Removes and returns the least element of the queue in log(size) time.
     *
     * @return the least element of the queue in log(size) time while removing it.
     */
    public final int pop() {
        if (size > 0) {
            int result = heap[1];    // save first value
            heap[1] = heap[size];    // move last to first
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
    public final int size() {
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
    }

    /**
     * Updates the heap because the cost of an element has changed, possibly from the outside.
     * Cost is linear with the size of the queue.
     */
    public final void update(int element) {
        int pos = findElementPosition(element);
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

    private int findElementPosition(int element) {
        final int limit = size + 1;
        final int[] data = heap;
        int i = 1;
        for (; i <= limit - 4; i += 4) {
            if (data[i] == element) return i;
            if (data[i + 1] == element) return i + 1;
            if (data[i + 2] == element) return i + 2;
            if (data[i + 3] == element) return i + 3;
        }
        for (; i < limit; ++i) {
            if (data[i] == element) return i;
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
    }

    private boolean upHeap(int origPos) {
        int i = origPos;
        int node = heap[i];          // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j])) {
            heap[i] = heap[j];       // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap[i] = node;              // install saved node
        return i != origPos;
    }

    private void downHeap(int i) {
        int node = heap[i];          // save top node
        int j = i << 1;              // find smaller child
        int k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j])) {
            j = k;
        }
        while (j <= size && lessThan(heap[j], node)) {
            heap[i] = heap[j];       // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j])) {
                j = k;
            }
        }
        heap[i] = node;              // install saved node
    }

    private void ensureCapacityForInsert() {
        if (size >= heap.length) {
            final int oversize = ArrayUtil.oversize(size + 1, Integer.BYTES);
            heap = Arrays.copyOf(
                    heap,
                    oversize);
        }
    }

    @Override
    public PrimitiveIntIterator iterator() {
        return new PrimitiveIntIterator() {

            int i = 1;

            @Override
            public boolean hasNext() {
                return i <= size;
            }

            /**
             * @throws ArrayIndexOutOfBoundsException when the iterator is exhausted.
             */
            @Override
            public int next() {
                return heap[i++];
            }
        };
    }

    public static IntPriorityQueue min(int capacity) {
        return new AbstractPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(int a, int b) {
                return costs.get(a) < costs.get(b);
            }
        };
    }

    public static IntPriorityQueue max(int capacity) {
        return new AbstractPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(int a, int b) {
                return costs.get(a) > costs.get(b);
            }
        };
    }

    public static IntPriorityQueue min() {
        return min(DEFAULT_CAPACITY);
    }

    public static IntPriorityQueue max() {
        return max(DEFAULT_CAPACITY);
    }

    private static abstract class AbstractPriorityQueue extends IntPriorityQueue {

        protected final IntDoubleScatterMap costs;

        public AbstractPriorityQueue(int initialCapacity) {
            super(initialCapacity);
            this.costs = new IntDoubleScatterMap(initialCapacity);
        }

        @Override
        protected boolean addCost(int element, double cost) {
            return costs.put(element, cost) != 0d;
        }

        @Override
        protected void removeCost(final int element) {
            costs.remove(element);
        }

        @Override
        protected double cost(int element) {
            return costs.get(element);
        }

        @Override
        public void clear() {
            super.clear();
            costs.clear();
        }

        @Override
        public void release() {
            super.release();
            costs.keys = EMPTY_INT;
            costs.clear();
            costs.keys = null;
            costs.values = null;
        }
    }

}
