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
        // The index which we will eventually use to
        // insert the message into the nodes' queue.
        long idx;

        outer:
        while (true) {
            idx = tails.get(nodeId);
            if (idx < 0) {
                // A negative index indicates that another thread
                // currently grows the queue for the given node id.
                // When the thread is done growing, the index will
                // turn positive again, so we go ahead and try to
                // set the next index.
                var nextId = -idx + 1;

                while (true) {
                    var currentIdx = tails.compareAndExchange(nodeId, -idx, nextId);
                    if (currentIdx == -idx) {
                        // The queue is grown and the current thread
                        // was successful setting the next index.
                        // We are done and can use the index to insert
                        // our message into the queue.
                        idx = -idx;
                        break outer;
                    }
                    if (currentIdx != idx) {
                        // The queue is grown but another thread beat
                        // us in setting the next possible index.
                        // We need to retry from the most outer loop.
                        continue outer;
                    }
                    // The grow thread is still ongoing, we continue
                    // trying to set the next index.
                }
            }
            // We basically perform and getAndIncrement and try
            // to update the tail with the next index.
            long nextIdx = idx + 1;

            if (hasSpaceLeft(nodeId, (int) nextIdx)) {
                // There is still room in the local queue.
                // We try to set our next index.
                long currentIdx = tails.compareAndExchange(nodeId, idx, nextIdx);
                if (currentIdx == idx) {
                    // CAX successful, we can go ahead and use our
                    // index to insert the message into the local queue.
                    break;
                }
            } else {
                // We need to grow the local queue. To indicate this and
                // block other insert threads, we set the negated
                // next index. Threads seeing this negative index will
                // spin in the upper loop.
                long currentIdx = tails.compareAndExchange(nodeId, idx, -nextIdx);
                if (currentIdx == idx) {
                    // Only a single thread gets into this block.
                    // We grow the queue and make sure there is
                    // enough space for the next index.
                    grow(nodeId, (int) nextIdx);
                    // We turn the index back to the positive value
                    // to indicate to waiting threads that we're
                    // done growing the local queue.
                    tails.compareAndExchange(nodeId, -nextIdx, nextIdx);
                    // Done. We can use the index to insert our message.
                    break;
                }
            }
        }

        // We place a full fence in order to make sure that writes after the
        // fence are not re-ordered with reads before the fence. In particular,
        // we avoid the queues.get call being moved before the grow operation
        // in order to avoid reading from the queue before it is grown.
        VarHandle.fullFence();
        // Set the message value at the computed index.
        ARRAY_HANDLE.setVolatile(queues.get(nodeId), (int) idx, message);
    }

    private boolean hasSpaceLeft(long nodeId, int minCapacity) {
        return queues.get(nodeId).length >= minCapacity;
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
