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
package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

import java.util.NoSuchElementException;


/**
 * A PriorityQueue specialized for ints that maintains a partial ordering of
 * its elements such that the smallest value can always be found in constant time.
 * Put()'s and pop()'s require log(size) time but the remove() cost implemented here is linear.
 * <p>
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @author phorn@avantgarde-labs.de
 */
public abstract class LongPriorityQueue implements PrimitiveLongIterable {
    private static final int DEFAULT_CAPACITY = 14;
    private int size = 0;
    private long[] heap;

    protected final LongDoubleScatterMap costs;

    public LongPriorityQueue() {
        this(DEFAULT_CAPACITY);
    }

    public LongPriorityQueue(final int initialCapacity) {
        final int heapSize;
        if (0 == initialCapacity) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based.  heap[0] is unused.
            heapSize = initialCapacity + 1;
        }
        this.heap = new long[ArrayUtil.oversize(heapSize, Integer.BYTES)];
        this.costs = new LongDoubleScatterMap(heapSize);
    }

    protected abstract boolean lessThan(long a, long b);

    /**
     * Adds an int associated with the given weight to a queue in log(size) time.
     *
     * @return the new 'top' element in the queue.
     */
    public long add(long element, double cost) {
        size++;
        ensureCapacityForInsert();
        heap[size] = element;
        costs.put(element, cost);
        upHeap(size);
        return heap[1];
    }

    public double getCost(long element) {
        return costs.get(element);
    }

    /**
     * @return the least element of the queue in constant time.
     */
    public long top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap[1];
    }

    public double topCost() {
        return costs.get(top());
    }

    /**
     * Removes and returns the least element of the queue in log(size) time.
     *
     * @return the least element of the queue in log(size) time while removing it.
     */
    public long pop() {
        if (size > 0) {
            long result = heap[1];       // save first value
            heap[1] = heap[size];     // move last to first
            size--;
            downHeap(1);              // adjust heap
            costs.remove(result);
            return result;
        } else {
            return -1;
        }
    }

    /**
     * @return the number of elements currently stored in the queue.
     */
    public int size() {
        return size;
    }

    /**
     * @return true iff there are currently no elements stored in the queue.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * @return true iff there is currently at least one element stored in the queue.
     */
    public boolean nonEmpty() {
        return size != 0;
    }

    /**
     * Removes all entries from the queue.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Removes all entries from the queue, releases all buffers.
     * The queue can no longer be used afterwards.
     */
    public void release() {
        size = 0;
        heap = null;

        costs.keys = new long[0];
        costs.clear();
        costs.keys = null;
        costs.values = null;
    }

    private boolean upHeap(int origPos) {
        int i = origPos;
        long node = heap[i];          // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j])) {
            heap[i] = heap[j];       // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap[i] = node;            // install saved node
        return i != origPos;
    }

    private void downHeap(int i) {
        long node = heap[i];          // save top node
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
        heap[i] = node;            // install saved node
    }

    private void ensureCapacityForInsert() {
        if (size >= heap.length) {
            long[] newHeap = new long[ArrayUtil.oversize(
                    size + 1,
                    Integer.BYTES)];
            System.arraycopy(heap, 0, newHeap, 0, heap.length);
            heap = newHeap;
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

            @Override
            public long next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return heap[i++];
            }
        };
    }

    public static LongPriorityQueue min(int capacity) {
        return new LongPriorityQueue() {
            @Override
            protected boolean lessThan(long a, long b) {
                return costs.get(a) < costs.get(b);
            }
        };
    }

    public static LongPriorityQueue max(int capacity) {
        return new LongPriorityQueue() {
            @Override
            protected boolean lessThan(long a, long b) {
                return costs.get(a) > costs.get(b);
            }
        };
    }

    public static LongPriorityQueue min() {
        return min(DEFAULT_CAPACITY);
    }

    public static LongPriorityQueue max() {
        return max(DEFAULT_CAPACITY);
    }
}
