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
package org.neo4j.gds.beta.pregel;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class PrimitiveDoubleQueues {
    // Used to insert into a single message queue array.
    private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);
    // Minimum capacity for the individual queue arrays.
    static final int MIN_CAPACITY = 42;
    // 🦀
    // Used to allow either a single thread exclusive access to a queue
    // in order to grow and replace it or multiple threads shared access
    // to the queue in order to insert a new message.
    private final HugeAtomicLongArray referenceCounts;

    // Manages a queue (double array) for each node.
    HugeObjectArray<double[]> queues;
    // Stores the tail indexes for each queue. The tail
    // index is used to insert a new message during push.
    HugeAtomicLongArray tails;

    PrimitiveDoubleQueues(
        HugeObjectArray<double[]> queues,
        HugeAtomicLongArray tails,
        HugeAtomicLongArray referenceCounts
    ) {
        this.tails = tails;
        this.queues = queues;
        this.referenceCounts = referenceCounts;
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
                // block other threads, we set the negated next index.
                // Threads seeing this negative index will spin in the upper loop.
                long currentIdx = tails.compareAndExchange(nodeId, idx, -nextIdx);
                if (currentIdx == idx) {
                    // Only a single thread gets into this block.
                    // We grow the queue and make sure there is
                    // enough space for the next index.

                    // We need to get exclusive access to the queue
                    // since we will grow and replace it. We have to
                    // make sure that no other thread is currently
                    // inserting into the queue.
                    getExclusiveReference(nodeId);
                    grow(nodeId, (int) nextIdx);
                    dropExclusiveReference(nodeId);

                    // We turn the index back to the positive value to notify
                    // waiting threads that we're done growing the local queue.
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

        // Multiple threads can concurrently update the queue, we need
        // to signal this with a shared reference to the array.
        getSharedReference(nodeId);
        ARRAY_HANDLE.setVolatile(queues.get(nodeId), (int) idx, message);
        dropSharedReference(nodeId);
    }

    private void getSharedReference(long nodeId) {
        while (true) {
            // If another thread is currently growing the queue, the
            // reference count will be negative. We need to wait until
            // this thread is finished and drops the exclusive reference.
            var refCount = referenceCounts.get(nodeId);
            if (refCount < 0) continue;

            // We increment the reference count by 1 to indicate that we
            // want to add a shared reference to the queue in order to
            // insert our message.
            if (referenceCounts.compareAndSet(nodeId, refCount, refCount + 1)) {
                break;
            }
        }
    }

    private void dropSharedReference(long nodeId) {
        // We decrement the reference count by 1 to indicate
        // that we finished updating the queue.
        referenceCounts.getAndAdd(nodeId, -1);
    }

    private void getExclusiveReference(long nodeId) {
        while (true) {
            // If other threads concurrently insert into the queue,
            // the reference count will be positive. We need to wait
            // until those threads finished before we can continue.
            var refCount = referenceCounts.get(nodeId);
            if (refCount > 0) {
                continue;
            }
            // Setting the reference to a negative value signals that
            // the queue is currently growing and must not be accessed.
            if (referenceCounts.compareAndSet(nodeId, refCount, -1)) {
                break;
            }
        }
    }

    private void dropExclusiveReference(long nodeId) {
        // We reset the reference count to 0
        // to signal other threads that the queue
        // is grown and can be used for inserting new
        // messages.
        referenceCounts.set(nodeId, 0);
    }

    private boolean hasSpaceLeft(long nodeId, int minCapacity) {
        return queues.get(nodeId).length >= minCapacity;
    }

    void release() {
        this.queues.release();
        this.tails.release();
        this.referenceCounts.release();
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
