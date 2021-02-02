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

import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

class AsyncQueueMessenger implements Messenger<AsyncQueueMessenger.AsyncIterator> {

    private final Graph graph;
    private final PregelConfig config;

    private final PrimitiveAsyncDoubleQueues queues;

    AsyncQueueMessenger(Graph graph, PregelConfig config, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.queues = new PrimitiveAsyncDoubleQueues(graph.nodeCount(), tracker);
    }

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("", (dimensions, concurrency) ->
            MemoryEstimations.builder(AsyncQueueMessenger.class)
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
        queues.init(iteration);
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        queues.push(targetNodeId, message);
    }

    @Override
    public AsyncIterator messageIterator() {
        return new AsyncIterator(queues);
    }

    @Override
    public void initMessageIterator(
        AsyncIterator messageIterator,
        long nodeId,
        boolean isFirstIteration
    ) {
        messageIterator.init(nodeId);
    }

    @Override
    public void release() {
        queues.release();
    }

    public static class AsyncIterator implements Messages.MessageIterator {

        private final PrimitiveAsyncDoubleQueues queues;
        private long nodeId;

        public AsyncIterator(PrimitiveAsyncDoubleQueues queues) {this.queues = queues;}

        void init(long nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public boolean hasNext() {
            return !queues.isEmpty(nodeId);
        }

        @Override
        public double nextDouble() {
            return queues.pop(nodeId);
        }

        @Override
        public boolean isEmpty() {
            return queues.isEmpty(nodeId);
        }
    }

}
