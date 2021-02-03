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
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

abstract class PrimitiveDoubleQueues {
    // used to store a message in a queue
    private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);
    // minimum capacity for the individual arrays
    static final int MIN_CAPACITY = 42;
    
    // toggling in-between super steps
    HugeObjectArray<double[]> queues;
    HugeAtomicLongArray tails;

    PrimitiveDoubleQueues(HugeAtomicLongArray tails, HugeObjectArray<double[]> queues) {
        this.tails = tails;
        this.queues = queues;
    }

    abstract void grow(long nodeId, int newCapacity);

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

    private boolean hasSpaceLeft(long nodeId, int minCapacity) {
        return queues.get(nodeId).length >= minCapacity;
    }

    private void set(double[] queue, int index, double message) {
        ARRAY_HANDLE.setVolatile(queue, index, message);
    }

    void release() {
        this.queues.release();
        this.tails.release();
    }

    @TestOnly
    long tail(long nodeId) {
        return tails.get(nodeId);
    }

    @TestOnly
    double[] queue(long nodeId) {
        return queues.get(nodeId);
    }
}
