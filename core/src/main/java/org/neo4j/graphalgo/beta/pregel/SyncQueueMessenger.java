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
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public class SyncQueueMessenger implements Messenger<SyncQueueMessenger.QueueIterator> {

    private final PrimitiveDoubleQueues messageQueues;

    SyncQueueMessenger(Graph graph, AllocationTracker tracker) {
        int averageDegree = (int) BitUtil.ceilDiv(graph.relationshipCount(), graph.nodeCount());
        this.messageQueues = new PrimitiveDoubleQueues(graph.nodeCount(), averageDegree, tracker);
    }

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("", (dimensions, concurrency) ->
            MemoryEstimations.builder(SyncQueueMessenger.class)
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
    public SyncQueueMessenger.QueueIterator messageIterator() {
        return new QueueIterator();
    }

    @Override
    public void initMessageIterator(SyncQueueMessenger.QueueIterator messageIterator, long nodeId, boolean isFirstIteration) {
        messageQueues.initIterator(messageIterator, nodeId);
    }

    @Override
    public void release() {
        messageQueues.release();
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
