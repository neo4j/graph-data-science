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
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Queue;
import java.util.stream.LongStream;

public class QueueMessenger implements Messenger {

    // Marks the end of messages from the previous iteration in synchronous mode.
    private static final Double TERMINATION_SYMBOL = Double.NaN;

    private final Graph graph;
    private final PregelConfig config;

    private final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues;

    private HugeAtomicBitSet messageBits;

    QueueMessenger(Graph graph, PregelConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.messageQueues = initQueues(graph, config.concurrency(), tracker);
    }

    private HugeObjectArray<MpscLinkedQueue<Double>> initQueues(
        Graph graph,
        int concurrency,
        AllocationTracker tracker
    ) {
        // sad java 😞
        Class<MpscLinkedQueue<Double>> queueClass = (Class<MpscLinkedQueue<Double>>) new MpscLinkedQueue<Double>().getClass();

        HugeObjectArray<MpscLinkedQueue<Double>> messageQueues = HugeObjectArray.newArray(
            queueClass,
            graph.nodeCount(),
            tracker
        );

        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, graph.nodeCount()),
            concurrency,
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
    public Messages.MessageIterator messageIterator() {
        return config.isAsynchronous()
            ? new Messages.MessageIterator.Async()
            : new Messages.MessageIterator.Sync();
    }

    @Override
    public void initMessageIterator(Messages.MessageIterator messageIterator, long nodeId) {
        messageIterator.init(receiveMessages(nodeId));
    }

    @Override
    public void release() {
        messageQueues.release();
    }

    private @Nullable Queue<Double> receiveMessages(long nodeId) {
        return messageBits.get(nodeId) ? messageQueues.get(nodeId) : null;
    }
}
