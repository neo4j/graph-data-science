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
package org.neo4j.graphalgo.beta.pregel;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

public class PrimitiveAsyncDoubleQueues {
    public static final double COMPACT_THRESHOLD = 0.25;
    // used to store a message in a queue
    private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);
    // minimum capacity for the individual arrays
    private static final int MIN_CAPACITY = 42;

    // toggling in-between super steps
    private HugeObjectArray<double[]> queues;
    private HugeAtomicLongArray heads;
    private HugeAtomicLongArray tails;

    public PrimitiveAsyncDoubleQueues(long nodeCount, AllocationTracker tracker) {
        this(nodeCount, MIN_CAPACITY, tracker);
    }

    public PrimitiveAsyncDoubleQueues(long nodeCount, int initialCapacity, AllocationTracker tracker) {
        this.heads = HugeAtomicLongArray.newArray(nodeCount, tracker);
        this.tails = HugeAtomicLongArray.newArray(nodeCount, tracker);

        this.queues = HugeObjectArray.newArray(double[].class, nodeCount, tracker);

        var capacity = Math.max(initialCapacity, MIN_CAPACITY);
        queues.setAll(value -> {
            var queue = new double[capacity];
            Arrays.fill(queue,Double.NaN);
            return queue;
        });
    }

    void init(int iteration) {
        compact();
    }

    void compact() {
        for (long i = 0; i < queues.size(); i++) {
            var queue = queues.get(i);
            var tail = (int) tails.get(i);
            var head = (int) heads.get(i);


            if (isEmpty(i) && head(i) > 0) {
                Arrays.fill(queue, 0, (int) tails.get(i), Double.NaN);
                heads.set(i, 0);
                tails.set(i, 0);
            } else if (head > queue.length * COMPACT_THRESHOLD) {
                var length = tail - head;
                System.arraycopy(queue, head, queue, 0, length);
                Arrays.fill(queue, length, queue.length, Double.NaN);

                heads.set(i, 0);
                tails.set(i, length);
            }
        }
    }

    void initIterator(SyncQueueMessenger.QueueIterator iterator, long nodeId) {
//        iterator.init(prevQueues.get(nodeId), (int) prevTails.get(nodeId));
    }

    boolean isEmpty(long nodeId) {
        var head = heads.get(nodeId);
        var tail = tails.get(nodeId);
        var queue = queues.get(nodeId);

        return head > tail || Double.isNaN(queue[(int) head]);
    }

    double pop(long nodeId) {
        var currentHead = heads.get(nodeId);
        heads.set(nodeId, currentHead + 1);
        return queues.get(nodeId)[(int) currentHead];
    }

    public void push(long nodeId, double message) {
        // get tail index
        long idx;

        outer:
        while (true) {
            idx = tails.get(nodeId);
            if (idx < 0) {
                var nextId = -idx + 1;

                while (true) {
                    var currentIdx = tails.compareAndExchange(nodeId, -idx, nextId);
                    if (currentIdx == -idx) {
                        // queue is grown
                        idx = -idx;
                        break outer;
                    }

                    if (currentIdx != idx) {
                        continue outer;
                    }
                }

            }
            long nextIdx = idx + 1;

            if (hasSpaceLeft(nodeId, (int) nextIdx)) {
                long currentIdx = tails.compareAndExchange(nodeId, idx, nextIdx);
                if (currentIdx == idx) {
                    // CAS successful
                    break;
                }
            } else {
                long currentIdx = tails.compareAndExchange(nodeId, idx, -nextIdx);
                if (currentIdx == idx) {
                    // one thread gets here and grows the queue
                    grow(nodeId, (int) nextIdx);
                    // set a positive value to signal other threads
                    // that the queue has grown
                    tails.compareAndExchange(nodeId, -nextIdx, nextIdx);
                    break;
                }
            }
        }

        VarHandle.fullFence();
        // set value
        set(queues.get(nodeId), (int) idx, message);

    }

    @TestOnly
    double[] queue(long nodeId) {
        return queues.get(nodeId);
    }

    @TestOnly
    long head(long nodeId) {
        return heads.get(nodeId);
    }

    @TestOnly
    long tail(long nodeId) {
        return tails.get(nodeId);
    }

    private boolean hasSpaceLeft(long nodeId, int minCapacity) {
        return queues.get(nodeId).length >= minCapacity;
    }

    private void grow(long nodeId, int minCapacity) {
        var queue = this.queues.get(nodeId);
        var capacity = queue.length;
        if (capacity >= minCapacity) {
            // some other thread already grew the array
            return;
        }
        // grow by 50%
        var newCapacity = capacity + (capacity >> 1);

        var resizedArray = Arrays.copyOf(queue, newCapacity);
        Arrays.fill(resizedArray, (int) -tails.get(nodeId), newCapacity, Double.NaN);

        this.queues.set(nodeId, resizedArray);
    }

    private void set(double[] queue, int index, double message) {
        ARRAY_HANDLE.setVolatile(queue, index, message);
    }

    void release() {
        this.heads.release();
        this.tails.release();
        this.queues.release();
    }
}
