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
package org.neo4j.gds.core.utils.queue;

import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.PrimitiveIterator;

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


    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(HugeLongPriorityQueue.class)
            .perNode("heap", HugeLongArray::memoryEstimation)
            .perNode("costs", HugeDoubleArray::memoryEstimation)
            .perNode("inverted index", HugeLongArray::memoryEstimation)
            .build();
    }

    private final long capacity;

    private HugeLongArray heap;
    private HugeLongArray mapIndexTo;
    private long size = 0;

    protected HugeDoubleArray costValues;

    /**
     * Creates a new priority queue with the given capacity.
     * The size is fixed, the queue cannot shrink or grow.
     */
    protected HugeLongPriorityQueue(long capacity) {
        long heapSize;
        if (0 == capacity) {
            // We allocate 1 extra to avoid if statement in top()
            heapSize = 2;
        } else {
            // NOTE: we add +1 because all access to heap is
            // 1-based not 0-based.  heap[0] is unused.
            heapSize = capacity + 1;
        }
        this.capacity = capacity;
        this.heap = HugeLongArray.newArray(heapSize);
        this.mapIndexTo = HugeLongArray.newArray(heapSize);
        this.costValues = HugeDoubleArray.newArray(capacity);
    }

    /**
     * Adds the element at the specified position in the heap array
     */
    private void placeElement(long position, long element) {
        heap.set(position, element);
        mapIndexTo.set(element, position);
    }

    /**
     * Adds an element associated with a cost to the queue in log(size) time.
     */
    public void add(long element, double cost) {
        assert element < capacity;
        addCost(element, cost);
        size++;
        placeElement(size, element);
        upHeap(size);
    }

    /**
     * Adds an element associated with a cost to the queue in log(size) time.
     * If the element was already in the queue, it's cost are updated and the
     * heap is reordered in log(size) time.
     */
    public void set(long element, double cost) {
        assert element < capacity;
        if (addCost(element, cost)) {
            update(element);
        } else {
            size++;
            placeElement(size, element);
            upHeap(size);
        }
    }

    /**
     * Returns the cost associated with the given element.
     * If the element has been popped from the queue, its
     * latest cost value is being returned.
     *
     * @return The double cost value for the element. 0.0D if the element is not found.
     */
    public double cost(long element) {
        return costValues.get(element);
    }

    /**
     * Returns true, iff the element is contained in the queue.
     */
    public boolean containsElement(long element) {
        return mapIndexTo.get(element) > 0;
    }

    /**
     * Returns the element with the minimum cost from the queue in constant time.
     */
    public long top() {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap.get(1);
    }

    /**
     * Removes and returns the element with the minimum cost from the queue in log(size) time.
     */
    public long pop() {
        if (size > 0) {
            long result = heap.get(1);    // save first value
            placeElement(1, heap.get(size));    // move last to first
            size--;
            downHeap(1);           // adjust heap
            removeCost(result);
            return result;
        } else {
            return -1;
        }
    }

    /**
     * Returns the number of elements currently stored in the queue.
     */
    public long size() {
        return size;
    }

    /**
     * Removes all entries from the queue, releases all buffers.
     * The queue can no longer be used afterwards.
     */
    public void release() {
        size = 0;
        heap = null;
        mapIndexTo = null;
        costValues.release();
    }

    /**
     * Defines the ordering of the queue.
     * Returns true iff {@code a} is strictly less than {@code b}.
     * <p>
     * The default behavior assumes a min queue, where the value with smallest cost is on top.
     * To implement a max queue, return {@code b < a}.
     * The resulting order is not stable.
     */
    protected abstract boolean lessThan(long a, long b);

    /**
     * Adds the given element to the queue.
     * If the element already exists, it's cost is overridden.
     *
     * @return true, if the element already existed, false otherwise.
     */
    private boolean addCost(long element, double cost) {
        boolean elementExists = mapIndexTo.get(element) > 0;
        costValues.set(element, cost);
        return elementExists;
    }

    /**
     * @return true iff there are currently no elements stored in the queue.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Removes all entries from the queue.
     */
    public void clear() {
        size = 0;
        mapIndexTo.fill(0L);
    }

     long findElementPosition(long element) {
         return mapIndexTo.get(element);

     }

    private boolean upHeap(long origPos) {
        long i = origPos;
        // save bottom node
        long node = heap.get(i);
        // find parent of current node
        long j = i >>> 1;
        while (j > 0 && lessThan(node, heap.get(j))) {
            // shift parents down
            placeElement(i, heap.get(j));
            i = j;
            // find new parent of swapped node
            j = j >>> 1;
        }
        // install saved node
        placeElement(i, node);
        return i != origPos;
    }

    private void downHeap(long i) {
        // save top node
        long node = heap.get(i);
        // find smallest child of top node
        long j = i << 1;
        long k = j + 1;
        if (k <= size && lessThan(heap.get(k), heap.get(j))) {
            j = k;
        }
        while (j <= size && lessThan(heap.get(j), node)) {
            // shift up child
            placeElement(i, heap.get(j));
            i = j;
            // find smallest child of swapped node
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap.get(k), heap.get(j))) {
                j = k;
            }
        }
        // install saved node
        placeElement(i, node);
    }

    private void update(long element) {
        long pos = findElementPosition(element);
        if (pos != 0) {
            if (!upHeap(pos) && pos < size) {
                downHeap(pos);
            }
        }
    }

    private void removeCost(long element) {
        mapIndexTo.set(element, 0);
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {

            int i = 1;

            @Override
            public boolean hasNext() {
                return i <= size;
            }

            /**
             * @throws ArrayIndexOutOfBoundsException when the iterator is exhausted.
             */
            @Override
            public long nextLong() {
                return heap.get(i++);
            }
        };
    }

    /**
     * Returns a non growing min priority queue,
     * i.e. the element with the lowest priority is always on top.
     */
    public static HugeLongPriorityQueue min(long capacity) {
        return new HugeLongPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costValues.get(a) < costValues.get(b);
            }
        };
    }

    /**
     * Returns a non growing max priority queue,
     * i.e. the element with the highest priority is always on top.
     */
    public static HugeLongPriorityQueue max(long capacity) {
        return new HugeLongPriorityQueue(capacity) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costValues.get(a) > costValues.get(b);
            }
        };
    }


    /**
     * Returns the element in the i-th position of the heap
     */
    public long getIth(int i) {
        return heap.get(i + 1);
    }

}
