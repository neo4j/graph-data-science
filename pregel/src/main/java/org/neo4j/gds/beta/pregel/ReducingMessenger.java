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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.OptionalLong;

/**
 * A messenger implementation that is backed by two double arrays used
 * to send and receive messages. The messenger can only be applied in
 * combination with a {@link Reducer}
 * which atomically reduces all incoming messages into a single one.
 */
class ReducingMessenger implements Messenger<ReducingMessenger.SingleMessageIterator> {

    private final Graph graph;
    private final PregelConfig config;
    final Reducer reducer;

    HugeAtomicDoubleArray sendArray;
    HugeAtomicDoubleArray receiveArray;

    static ReducingMessenger create(Graph graph, PregelConfig config, Reducer reducer) {
        return config.trackSender()
            ? new WithSender(graph, config, reducer)
            : new ReducingMessenger(graph, config, reducer);
    }

    private ReducingMessenger(Graph graph, PregelConfig config, Reducer reducer) {
        assert !Double.isNaN(reducer.identity()) : "identity element must not be NaN";

        this.graph = graph;
        this.config = config;
        this.reducer = reducer;

        this.receiveArray = HugeAtomicDoubleArray.of(
            graph.nodeCount(),
            ParallelDoublePageCreator.passThrough(config.concurrency())
        );
        this.sendArray = HugeAtomicDoubleArray.of(
            graph.nodeCount(),
            ParallelDoublePageCreator.passThrough(config.concurrency())
        );
    }

    static MemoryEstimation memoryEstimation(boolean withSender) {
        var builder = MemoryEstimations.builder(ReducingMessenger.class)
            .perNode("send array", HugeAtomicDoubleArray::memoryEstimation)
            .perNode("receive array", HugeAtomicDoubleArray::memoryEstimation);

        if (withSender) {
            builder
                .perNode("send sender array", HugeLongArray::memoryEstimation)
                .perNode("receive sender array", HugeLongArray::memoryEstimation);
        }
        return builder
            .build();
    }

    @Override
    public void initIteration(int iteration) {
        var tmp = receiveArray;
        this.receiveArray = sendArray;
        this.sendArray = tmp;

        var concurrency = config.concurrency();
        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            nodeId -> sendArray.set(nodeId, reducer.identity())
        );
    }

    @Override
    public void sendTo(long sourceNodeId, long targetNodeId, double message) {
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
        messageIterator.init(message, message != reducer.identity(), OptionalLong.empty());
    }

    @Override
    public void release() {
        sendArray.release();
        receiveArray.release();
    }

    static class WithSender extends ReducingMessenger {
        private HugeLongArray sendSenderArray;
        private HugeLongArray receiveSenderArray;

        WithSender(Graph graph, PregelConfig config, Reducer reducer) {
            super(graph, config, reducer);
            this.sendSenderArray = HugeLongArray.newArray(graph.nodeCount());
            this.receiveSenderArray = HugeLongArray.newArray(graph.nodeCount());
        }

        @Override
        public void initIteration(int iteration) {
            super.initIteration(iteration);
            // Swap sender arrays
            var tmp = receiveSenderArray;
            this.receiveSenderArray = sendSenderArray;
            this.sendSenderArray = tmp;
        }

        @Override
        public void initMessageIterator(
            ReducingMessenger.SingleMessageIterator messageIterator,
            long nodeId,
            boolean isInitialIteration
        ) {
            var message = receiveArray.getAndReplace(nodeId, reducer.identity());
            var sender = receiveSenderArray.get(nodeId);
            messageIterator.init(message, message != reducer.identity(), OptionalLong.of(sender));
        }

        @Override
        public void sendTo(long sourceNodeId, long targetNodeId, double message) {
            sendArray.update(
                targetNodeId,
                currentMessage -> {
                    var reducedMessage = reducer.reduce(currentMessage, message);
                    if (Double.compare(reducedMessage, currentMessage) != 0) {
                        sendSenderArray.set(targetNodeId, sourceNodeId);
                    }
                    return reducedMessage;
                }
            );
        }

        @Override
        public OptionalLong sender(long nodeId) {
            return OptionalLong.of(receiveSenderArray.get(nodeId));
        }

        @Override
        public void release() {
            sendSenderArray.release();
            receiveSenderArray.release();
            super.release();
        }
    }

    static class SingleMessageIterator implements Messages.MessageIterator {

        boolean hasNext;
        double message;
        OptionalLong sender;

        void init(double value, boolean hasNext, OptionalLong sender) {
            this.message = value;
            this.hasNext = hasNext;
            this.sender = sender;
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

        @Override
        public OptionalLong sender() {
            return this.sender;
        }
    }
}
