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
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Queue;
import java.util.stream.LongStream;

/**
 * A messenger implementation that is backed by an MPSC queue
 * for each node in the graph. The queue acts as message inbox.
 */
class QueueMessenger implements Messenger<QueueMessenger.QueueIterator> {

    // Marks the end of messages from the previous iteration in synchronous mode.
    private static final Double TERMINATION_SYMBOL = Double.NaN;

    private final Graph graph;
    private final PregelConfig config;

    private final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues;

    private HugeAtomicBitSet messageBits;

    QueueMessenger(Graph graph, PregelConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.messageQueues = initQueues(tracker);
    }

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("", (dimensions, concurrency) ->
            MemoryEstimations.builder(QueueMessenger.class)
                .fixed(HugeObjectArray.class.getSimpleName(), MemoryUsage.sizeOfInstance(HugeObjectArray.class))
                .perNode("node queue", MemoryEstimations.builder(MpscLinkedQueue.class)
                    .fixed("messages", dimensions.averageDegree() * Double.BYTES)
                    .build()
                )
                .build()
        );
    }

    private HugeObjectArray<MpscLinkedQueue<Double>> initQueues(AllocationTracker tracker) {
        // sad java 😞
        Class<MpscLinkedQueue<Double>> queueClass = (Class<MpscLinkedQueue<Double>>) new MpscLinkedQueue<Double>().getClass();

        HugeObjectArray<MpscLinkedQueue<Double>> messageQueues = HugeObjectArray.newArray(
            queueClass,
            graph.nodeCount(),
            tracker
        );

        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, graph.nodeCount()),
            config.concurrency(),
            nodeIds -> nodeIds.forEach(nodeId -> messageQueues.set(nodeId, new MpscLinkedQueue<Double>()))
        );

        return messageQueues;
    }

    @Override
    public void initIteration(int iteration, HugeAtomicBitSet messageBits) {
        this.messageBits = messageBits;

        if (!config.isAsynchronous()) {
            // Synchronization barrier:
            // Add termination flag to message queues that
            // received messages in the previous iteration.
            if (iteration > 0) {
                ParallelUtil.parallelStreamConsume(
                    LongStream.range(0, graph.nodeCount()),
                    config.concurrency(),
                    nodeIds -> nodeIds.forEach(nodeId -> {
                        if (messageBits.get(nodeId)) {
                            messageQueues.get(nodeId).add(TERMINATION_SYMBOL);
                        }
                    })
                );
            }
        }
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        messageQueues.get(targetNodeId).add(message);
    }

    @Override
    public QueueMessenger.QueueIterator messageIterator() {
        return config.isAsynchronous()
            ? new QueueIterator.Async()
            : new QueueIterator.Sync();
    }

    @Override
    public void initMessageIterator(QueueMessenger.QueueIterator messageIterator, long nodeId) {
        messageIterator.init(messageBits.get(nodeId) ? messageQueues.get(nodeId) : null);
    }

    @Override
    public void release() {
        messageQueues.release();
    }

    public abstract static class QueueIterator implements Messages.MessageIterator {

        Queue<Double> queue;

        void init(@Nullable Queue<Double> queue) {
            this.queue = queue;
        }

        @Override
        public Double next() {
            return queue.poll();
        }

        public abstract boolean isEmpty();

        static class Sync extends QueueIterator {
            private boolean reachedEnd = false;

            @Override
            void init(@Nullable Queue<Double> queue) {
                super.init(queue);
                reachedEnd = false;
            }

            @Override
            public boolean hasNext() {
                if (queue == null || reachedEnd) {
                    return false;
                }

                if (Double.isNaN(queue.peek())) {
                    queue.poll();
                    reachedEnd = true;
                    return false;
                }

                return true;
            }

            @Override
            public boolean isEmpty() {
                return queue == null || queue.isEmpty() || reachedEnd || Double.isNaN(queue.peek());
            }
        }

        static class Async extends QueueIterator {
            @Override
            public boolean hasNext() {
                if (queue == null) {
                    return false;
                }
                return (queue.peek()) != null;
            }


            @Override
            public boolean isEmpty() {
                return queue == null || queue.isEmpty();
            }
        }
    }

}
