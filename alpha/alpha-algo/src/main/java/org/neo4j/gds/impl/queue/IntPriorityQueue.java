/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.gds.impl.queue;

import com.carrotsearch.hppc.IntDoubleScatterMap;
import org.neo4j.gds.collections.ArrayUtil;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeIntArray;

/**
 * A PriorityQueue specialized for ints that maintains a partial ordering of
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
public abstract class IntPriorityQueue {

    private static final int DEFAULT_CAPACITY = 14;
    private static final int[] EMPTY_INT = new int[0];

    private HugeIntArray heap;
    private long size = 0;

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
        this.heap = HugeIntArray.newArray(ArrayUtil.oversizeHuge(heapSize, Integer.BYTES));
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
        heap.set(size, element);
        upHeap(size);
    }

    private void add(int element) {
        size++;
        ensureCapacityForInsert();
        heap.set(size, element);
        upHeap(size);
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public final int top() {
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
    public final int pop() {
        if (size > 0) {
            int result = heap.get(1);    // save first value
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
        final HugeIntArray data = heap;
        HugeCursor<int[]> cursor = data.initCursor(data.newCursor(), 1, limit);
        while (cursor.next()) {
            int[] internalArray = cursor.array;
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
    }

    private boolean upHeap(long origPos) {
        long i = origPos;
        int node = heap.get(i);          // save bottom node
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
        int node = heap.get(i);          // save top node
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
            long oversize = ArrayUtil.oversizeHuge(size + 1, Integer.BYTES);
            heap = heap.copyOf(oversize);
        }
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

    private abstract static class AbstractPriorityQueue extends IntPriorityQueue {

        protected final IntDoubleScatterMap costs;

        AbstractPriorityQueue(int initialCapacity) {
            super(initialCapacity);
            this.costs = new IntDoubleScatterMap(initialCapacity);
        }

        @Override
        protected boolean addCost(int element, double cost) {
            return costs.put(element, cost) != 0D;
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
