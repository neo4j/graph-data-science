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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;

/**
 * A messenger implementation that is backed by two double arrays used
 * to send and receive messages. The messenger can only be applied in
 * combination with a {@link org.neo4j.graphalgo.beta.pregel.Reducer}
 * which atomically reduces all incoming messages into a single one.
 */
public class ReducingMessenger implements Messenger<ReducingMessenger.SingleMessageIterator> {

    private final Graph graph;
    private final PregelConfig config;
    private final Reducer reducer;

    private HugeAtomicDoubleArray sendArray;
    private HugeAtomicDoubleArray receiveArray;

    ReducingMessenger(Graph graph, PregelConfig config, Reducer reducer, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.reducer = reducer;

        this.receiveArray = HugeAtomicDoubleArray.newArray(graph.nodeCount(), tracker);
        this.sendArray = HugeAtomicDoubleArray.newArray(graph.nodeCount(), tracker);
    }

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(ReducingMessenger.class)
            .perNode("send array", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("receive array", HugeAtomicDoubleArray::memoryEstimation)
            .build();
    }

    @Override
    public void initIteration(int iteration) {
        // Swap arrays
        var tmp = receiveArray;
        this.receiveArray = sendArray;
        this.sendArray = tmp;

        ParallelUtil.parallelForEachNode(
            graph,
            config.concurrency(),
            nodeId -> sendArray.set(nodeId, reducer.identity())
        );
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        sendArray.update(
            targetNodeId,
            current -> reducer.reduce(current, message)
        );
    }

    @Override
    public ReducingMessenger.SingleMessageIterator messageIterator() {
        return new SingleMessageIterator();
    }

    @Override
    public void initMessageIterator(
        ReducingMessenger.SingleMessageIterator messageIterator,
        long nodeId,
        boolean isInitialIteration
    ) {
        var message = receiveArray.getAndReplace(nodeId, reducer.identity());
        messageIterator.init(message, message != reducer.identity());
    }

    @Override
    public void release() {
        sendArray.release();
        receiveArray.release();
    }

    static class SingleMessageIterator implements Messages.MessageIterator {

        boolean hasNext;
        double message;

        void init(double value, boolean hasNext) {
            this.message = value;
            this.hasNext = hasNext;
        }

        @Override
        public boolean isEmpty() {
            return !hasNext;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public double nextDouble() {
            hasNext = false;
            return message;
        }
    }
}
