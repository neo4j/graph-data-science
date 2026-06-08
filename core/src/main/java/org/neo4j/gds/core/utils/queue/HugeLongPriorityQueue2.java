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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;

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
public abstract class HugeLongPriorityQueue2 implements PrimitiveLongIterable {


    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(HugeLongPriorityQueue2.class)
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
    protected HugeLongPriorityQueue2(long capacity) {
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

        if (isEmpty()) {
            throw new IndexOutOfBoundsException("Priority Queue is empty");
        }
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
        long newPos = origPos;
        // save bottom node
        long node = heap.get(newPos);
        // find parent of current node in a 4-ary heap: (i + 2) / 4
        long parentPos = (newPos + 2) >>> 2;
        while (parentPos > 0) {
            long parent = heap.get(parentPos);
            if (!lessThan(node, parent)) {
                break;
            }
            // shift parent down
            placeElement(newPos, parent);
            newPos = parentPos;
            // find new parent of swapped node
            parentPos = (parentPos + 2) >>> 2;
        }
        // install saved node
        placeElement(newPos, node);
        return newPos != origPos;
    }

    private void downHeap(long pos) {
        // hoist field read across the abstract lessThan call sites below
        long size = this.size;
        // save top node
        long node = heap.get(pos);
        // find first of up to four children: 4i - 2
        long firstChildPos = (pos << 2) - 2;
        long smallestChildPos = smallestChildPosition(firstChildPos, size);
        while (smallestChildPos <= size) {
            long smallestChild = heap.get(smallestChildPos);
            if (!lessThan(smallestChild, node)) {
                break;
            }
            // shift up smallest child
            placeElement(pos, smallestChild);
            pos = smallestChildPos;
            // find smallest child of swapped node
            firstChildPos = (pos << 2) - 2;
            smallestChildPos = smallestChildPosition(firstChildPos, size);
        }
        // install saved node
        placeElement(pos, node);
    }

    /**
     * Returns the index of the smallest among the up to four children
     * starting at {@code firstChild}. If no children exist, the returned
     * index is greater than {@code size}.
     */
    private long smallestChildPosition(long firstChild, long size) {
        if (firstChild > size) {
            return firstChild;
        }
        long smallest = firstChild;
        long smallestVal = heap.get(firstChild);
        long last = firstChild + 3;
        if (last > size) {
            last = size;
        }
        for (long k = firstChild + 1; k <= last; k++) {
            long candidate = heap.get(k);
            if (lessThan(candidate, smallestVal)) {
                smallest = k;
                smallestVal = candidate;
            }
        }
        return smallest;
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
    public static HugeLongPriorityQueue2 min(long capacity) {
        return new HugeLongPriorityQueue2(capacity) {
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
    public static HugeLongPriorityQueue2 max(long capacity) {
        return new HugeLongPriorityQueue2(capacity) {
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
