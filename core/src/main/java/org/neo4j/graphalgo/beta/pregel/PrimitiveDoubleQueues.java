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

public class PrimitiveDoubleQueues {
    // used to store a message in a queue
    private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);
    // minimum capacity for the individual arrays
    private static final int MIN_CAPACITY = 42;

    // toggling in-between super steps
    private HugeObjectArray<double[]> currentQueues;
    private HugeAtomicLongArray currentTails;

    private HugeObjectArray<double[]> prevQueues;
    private HugeAtomicLongArray prevTails;

    public PrimitiveDoubleQueues(long nodeCount, AllocationTracker tracker) {
        this(nodeCount, MIN_CAPACITY, tracker);
    }

    public PrimitiveDoubleQueues(long nodeCount, int initialCapacity, AllocationTracker tracker) {
        this.currentTails = HugeAtomicLongArray.newArray(nodeCount, tracker);
        this.prevTails = HugeAtomicLongArray.newArray(nodeCount, tracker);

        this.currentQueues = HugeObjectArray.newArray(double[].class, nodeCount, tracker);
        this.prevQueues = HugeObjectArray.newArray(double[].class, nodeCount, tracker);

        var capacity = Math.max(initialCapacity, MIN_CAPACITY);
        currentQueues.setAll(value -> new double[capacity]);
        prevQueues.setAll(value -> new double[capacity]);
    }

    void init(int iteration) {
        // swap tail indexes
        var tmpTails = currentTails;
        this.currentTails = prevTails;
        this.prevTails = tmpTails;
        // swap queues
        var tmpQueues = currentQueues;
        this.currentQueues = prevQueues;
        this.prevQueues = tmpQueues;

        if (iteration > 0) {
            this.currentTails.setAll(0);
        }
    }

    void initIterator(SyncQueueMessenger.QueueIterator iterator, long nodeId) {
        iterator.init(prevQueues.get(nodeId), (int) prevTails.get(nodeId));
    }

    public void push(long nodeId, double message) {
        // get tail index
        long idx;

        outer:
        while (true) {
            idx = currentTails.get(nodeId);
            if (idx < 0) {
                var nextId = -idx + 1;

                while (true) {
                    var currentIdx = currentTails.compareAndExchange(nodeId, -idx, nextId);
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
                long currentIdx = currentTails.compareAndExchange(nodeId, idx, nextIdx);
                if (currentIdx == idx) {
                    // CAS successful
                    break;
                }
            } else {
                long currentIdx = currentTails.compareAndExchange(nodeId, idx, -nextIdx);
                if (currentIdx == idx) {
                    // one thread gets here and grows the queue
                    grow(nodeId, (int) nextIdx);
                    // set a positive value to signal other threads
                    // that the queue has grown
                    currentTails.compareAndExchange(nodeId, -nextIdx, nextIdx);
                    break;
                }
            }
        }

        VarHandle.fullFence();
        // set value
        set(currentQueues.get(nodeId), (int) idx, message);

    }

    @TestOnly
    long tail(long nodeId) {
        return currentTails.get(nodeId);
    }

    @TestOnly
    double[] queue(long nodeId) {
        return currentQueues.get(nodeId);
    }

    private boolean hasSpaceLeft(long nodeId, int minCapacity) {
        return currentQueues.get(nodeId).length >= minCapacity;
    }

    private void grow(long nodeId, int minCapacity) {
        var queue = currentQueues.get(nodeId);
        var capacity = queue.length;
        if (capacity >= minCapacity) {
            // some other thread already grew the array
            return;
        }
        // grow by 50%
        var newCapacity = capacity + (capacity >> 1);
        currentQueues.set(nodeId, Arrays.copyOf(queue, newCapacity));
    }

    private void set(double[] queue, int index, double message) {
        ARRAY_HANDLE.setVolatile(queue, index, message);
    }

    void release() {
        this.currentTails.release();
        this.prevTails.release();
        this.currentQueues.release();
    }
}
