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

public abstract class HugeLongPriorityQueue implements PrimitiveLongIterable {


    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(HugeLongPriorityQueue.class)
            .perNode("heap", HugeLongArray::memoryEstimation)
            .perNode("costs", HugeDoubleArray::memoryEstimation)
            .perNode("heap costs", HugeDoubleArray::memoryEstimation)
            .perNode("inverted index", HugeLongArray::memoryEstimation)
            .build();
    }

    private final long capacity;

    private final HugeLongArray heap;
    private final HugeLongArray mapIndexTo;
    protected final HugeDoubleArray costValues;
    /**
     * Mirrors {@link #costValues} but is indexed by heap <em>position</em> rather than by
     * element. Keeping a node's cost next to its heap slot turns the cost reads done during
     * sifting into cache-local accesses (a node's up-to-four children live in contiguous
     * positions) instead of scattered, element-indexed lookups into {@link #costValues}.
     */
    private final HugeDoubleArray heapCosts;

    private long size = 0;

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
        this.heapCosts = HugeDoubleArray.newArray(heapSize);
    }

    /**
     * Places {@code element} (with its already-known {@code cost}) at the given heap position,
     * keeping the element-&gt;position index and the position-indexed cost mirror in sync.
     */
    private void placeElement(long position, long element, double cost) {
        heap.set(position, element);
        mapIndexTo.set(element, position);
        heapCosts.set(position, cost);
    }

    /**
     * Adds an element associated with a cost to the queue in log(size) time.
     */
    public void add(long element, double cost) {
        assert element < capacity;
        addCost(element, cost);
        size++;
        placeElement(size, element, cost);
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
            placeElement(size, element, cost);
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
            // move last to first, carrying its cached cost so we avoid an element-indexed lookup
            placeElement(1, heap.get(size), heapCosts.get(size));
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
     * Defines the ordering of the queue.
     * Returns true iff element {@code a} is strictly less than element {@code b}.
     * <p>
     * Both the element id and its associated cost are supplied for each operand. The cost is
     * passed in (read cache-locally from the position-indexed mirror) so implementations can
     * avoid an element-indexed {@link #costValues} lookup; the element id is still available
     * for orderings that depend on it (e.g. a heuristic function or a tie-break on id).
     * <p>
     * The default behavior assumes a min queue, where the value with smallest cost is on top.
     * To implement a max queue, return {@code costB < costA}.
     * The resulting order is not stable.
     */
    protected abstract boolean lessThan(long a, double costA, long b, double costB);

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
        // save bottom node and its cost
        long node = heap.get(newPos);
        double nodeCost = heapCosts.get(newPos);
        // find parent of current node in a 4-ary heap: (i + 2) / 4
        long parentPos = (newPos + 2) >>> 2;
        while (parentPos > 0) {
            long parent = heap.get(parentPos);
            double parentCost = heapCosts.get(parentPos);
            if (!lessThan(node, nodeCost, parent, parentCost)) {
                break;
            }
            // shift parent down
            placeElement(newPos, parent, parentCost);
            newPos = parentPos;
            // find new parent of swapped node
            parentPos = (parentPos + 2) >>> 2;
        }
        // install saved node
        placeElement(newPos, node, nodeCost);
        return newPos != origPos;
    }

    private void downHeap(long pos) {
        // hoist field read across the abstract lessThan call sites below
        long size = this.size;
        // save top node and its cost
        long node = heap.get(pos);
        double nodeCost = heapCosts.get(pos);
        // find first of up to four children: 4i - 2
        long firstChildPos = (pos << 2) - 2;
        long smallestChildPos = smallestChildPosition(firstChildPos, size);
        while (smallestChildPos <= size) {
            long smallestChild = heap.get(smallestChildPos);
            double smallestChildCost = heapCosts.get(smallestChildPos);
            if (!lessThan(smallestChild, smallestChildCost, node, nodeCost)) {
                break;
            }
            // shift up smallest child
            placeElement(pos, smallestChild, smallestChildCost);
            pos = smallestChildPos;
            // find smallest child of swapped node
            firstChildPos = (pos << 2) - 2;
            smallestChildPos = smallestChildPosition(firstChildPos, size);
        }
        // install saved node
        placeElement(pos, node, nodeCost);
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
        long smallestElement = heap.get(firstChild);
        double smallestCost = heapCosts.get(firstChild);
        long last = firstChild + 3;
        if (last > size) {
            last = size;
        }
        for (long k = firstChild + 1; k <= last; k++) {
            long candidate = heap.get(k);
            double candidateCost = heapCosts.get(k);
            if (lessThan(candidate, candidateCost, smallestElement, smallestCost)) {
                smallest = k;
                smallestElement = candidate;
                smallestCost = candidateCost;
            }
        }
        return smallest;
    }

    private void update(long element) {
        long pos = findElementPosition(element);
        if (pos != 0) {
            // the cost in costValues was just changed; refresh the position-indexed mirror
            // before sifting so comparisons see the updated value
            heapCosts.set(pos, costValues.get(element));
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

            long i = 1;

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
            protected boolean lessThan(long a, double costA, long b, double costB) {
                return costA < costB;
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
            protected boolean lessThan(long a, double costA, long b, double costB) {
                return costA > costB;
            }
        };
    }


    /**
     * Returns the element in the i-th position of the heap
     */
    public long getIth(long i) {
        return heap.get(i + 1);
    }

}
