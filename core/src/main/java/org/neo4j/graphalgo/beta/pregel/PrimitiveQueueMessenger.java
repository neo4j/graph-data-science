/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

class PrimitiveQueueMessenger implements Messenger<PrimitiveQueueMessenger.QueueIterator> {

    private final DoubleArrayQueues messageQueues;

    PrimitiveQueueMessenger(Graph graph, AllocationTracker tracker) {
        int averageDegree = (int) BitUtil.ceilDiv(graph.relationshipCount(), graph.nodeCount());
        this.messageQueues = new DoubleArrayQueues(graph.nodeCount(), averageDegree, tracker);
    }

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("", (dimensions, concurrency) ->
            MemoryEstimations.builder(PrimitiveQueueMessenger.class)
                .fixed(HugeObjectArray.class.getSimpleName(), MemoryUsage.sizeOfInstance(HugeObjectArray.class))
                .perNode("node queue", MemoryEstimations.builder(MpscLinkedQueue.class)
                    .fixed("messages", dimensions.averageDegree() * Double.BYTES)
                    .build()
                )
                .build()
        );
    }

    @Override
    public void initIteration(int iteration) {
        messageQueues.init(iteration);
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        messageQueues.push(targetNodeId, message);
    }

    @Override
    public PrimitiveQueueMessenger.QueueIterator messageIterator() {
        return new QueueIterator();
    }

    @Override
    public void initMessageIterator(PrimitiveQueueMessenger.QueueIterator messageIterator, long nodeId, boolean isFirstIteration) {
        messageQueues.initIterator(messageIterator, nodeId);
    }

    @Override
    public void release() {
        messageQueues.release();
    }

    static class DoubleArrayQueues {
        // used to store a message in a queue
        private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(double[].class);
        // minimum capacity for the individual arrays
        private static final int MIN_CAPACITY = 42;
        // each queue is guarded by a lock which is used for growing the queue
        private final HugeObjectArray<ReentrantLock> queueLocks;

        // toggling in-between super steps
        private HugeObjectArray<double[]> currentQueues;
        private HugeAtomicLongArray currentTails;

        private HugeObjectArray<double[]> prevQueues;
        private HugeAtomicLongArray prevTails;

        DoubleArrayQueues(long nodeCount, AllocationTracker tracker) {
            this(nodeCount, MIN_CAPACITY, tracker);
        }

        DoubleArrayQueues(long nodeCount, int initialCapacity, AllocationTracker tracker) {
            // TODO: consider partitioning node space and use 1 lock per partition
            this.queueLocks = HugeObjectArray.newArray(ReentrantLock.class, nodeCount, tracker);
            queueLocks.setAll(ignored -> new ReentrantLock());

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

        void initIterator(QueueIterator iterator, long nodeId) {
            iterator.init(prevQueues.get(nodeId), (int) prevTails.get(nodeId));
        }

        void push(long nodeId, double message) {
            // get tail index
            long idx = currentTails.get(nodeId);
            while (true) {
                long nextIdx = idx + 1;
                long currentIdx = currentTails.compareAndExchange(nodeId, idx, nextIdx);
                if (currentIdx == idx) {
                    // CAS successful
                    break;
                }
                // CAS unsuccessful, try again
                idx = currentIdx;
            }
            // grow queue if necessary
            ensureCapacity(nodeId, (int) idx + 1);
            // set value
            set(currentQueues.get(nodeId), (int) idx, message);
        }

        private void ensureCapacity(long nodeId, int minCapacity) {
            if (currentQueues.get(nodeId).length < minCapacity) {
                grow(nodeId, minCapacity);
            }
        }

        private void grow(long nodeId, int minCapacity) {
            queueLocks.get(nodeId).lock();
            try {
                var queue = currentQueues.get(nodeId);
                var capacity = queue.length;
                if (capacity >= minCapacity) {
                    // some other thread already grew the array
                    return;
                }
                // grow by 50%
                var newCapacity = capacity + (capacity >> 1);
                currentQueues.set(nodeId, Arrays.copyOf(queue, newCapacity));
            } finally {
                queueLocks.get(nodeId).unlock();
            }
        }

        private void set(double[] queue, int index, double message) {
            ARRAY_HANDLE.setVolatile(queue, index, message);
        }

        void release() {
            this.queueLocks.release();
            this.currentTails.release();
            this.prevTails.release();
            this.currentQueues.release();
        }
    }

    public static class QueueIterator implements Messages.MessageIterator {

        double[] queue;
        private int length;
        private int pos;

        void init(double[] queue, int length) {
            this.queue = queue;
            this.pos = 0;
            this.length = length;
        }

        @Override
        public boolean hasNext() {
            return pos < length;
        }

        @Override
        public Double next() {
            return queue[pos++];
        }

        @Override
        public boolean isEmpty() {
            return length == 0;
        }
    }
}
