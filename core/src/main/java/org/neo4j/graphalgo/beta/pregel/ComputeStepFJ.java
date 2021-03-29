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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.concurrent.CountedCompleter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

public final class ComputeStepFJ<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator>
    extends CountedCompleter<Void>
    implements ComputeStep {

    private static final int SEQUENTIAL_THRESHOLD = 1000;

    private final Graph graph;
    private final CONFIG config;

    private final long nodeCount;
    private final long relationshipCount;
    private final boolean isMultiGraph;
    private final InitContext<CONFIG> initContext;
    private final ComputeContext<CONFIG> computeContext;
    private final Degrees degrees;
    private final NodeValue nodeValue;
    private final HugeAtomicBitSet voteBits;
    private final Messenger<ITERATOR> messenger;
    private final PregelComputation<CONFIG> computation;
    private final RelationshipIterator relationshipIterator;

    private Partition nodeBatch;
    private final int iteration;
    private boolean hasSendMessage;
    private final AtomicBoolean sentMessage;

    ComputeStepFJ(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        int iteration,
        Partition nodeBatch,
        NodeValue nodeValue,
        Messenger<ITERATOR> messenger,
        HugeAtomicBitSet voteBits,
        RelationshipIterator relationshipIterator,
        CountedCompleter<Void> parent,
        AtomicBoolean sentMessage
    ) {
        super(parent);
        this.graph = graph;
        this.config = config;
        this.iteration = iteration;
        this.nodeCount = graph.nodeCount();
        this.relationshipCount = graph.relationshipCount();
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.degrees = graph;
        this.isMultiGraph = graph.isMultiGraph();
        this.nodeValue = nodeValue;
        this.relationshipIterator = relationshipIterator.concurrentCopy();
        this.messenger = messenger;
        this.computeContext = new ComputeContext<>(this, config);
        this.sentMessage = sentMessage;
        this.initContext = new InitContext<>(this, config, graph);
    }

    @Override
    public void compute() {
        if (nodeBatch.nodeCount() >= SEQUENTIAL_THRESHOLD) {
            long startNode = nodeBatch.startNode();
            long batchSize = nodeBatch.nodeCount();
            boolean isEven = batchSize % 2 == 0;

            long pivot = BitUtil.ceilDiv(batchSize, 2);

            var rightBatch = isEven
                ? Partition.of(startNode + pivot, pivot)
                : Partition.of(startNode + pivot, pivot - 1);

            var leftBatch = Partition.of(startNode, pivot);

            var leftTask = new ComputeStepFJ<>(
                graph,
                computation,
                config,
                iteration,
                leftBatch,
                nodeValue,
                messenger,
                voteBits,
                relationshipIterator,
                this,
                sentMessage
            );

            this.nodeBatch = rightBatch;

            addToPendingCount(1);
            leftTask.fork();

            this.compute();
        } else {
            computeSequentially();
            tryComplete();
        }
    }

    private void computeSequentially() {
        var messageIterator = messenger.messageIterator();
        var messages = new Messages(messageIterator);

        long batchStart = nodeBatch.startNode();
        long batchEnd = batchStart + nodeBatch.nodeCount();

        for (long nodeId = batchStart; nodeId < batchEnd; nodeId++) {

            if (computeContext.isInitialSuperstep()) {
                initContext.setNodeId(nodeId);
                computation.init(initContext);
            }

            messenger.initMessageIterator(messageIterator, nodeId, computeContext.isInitialSuperstep());

            if (!messages.isEmpty() || !voteBits.get(nodeId)) {
                voteBits.clear(nodeId);
                computeContext.setNodeId(nodeId);
                computation.compute(computeContext, messages);
            }
        }

        this.sentMessage.set(hasSendMessage);
    }

    @Override
    public int iteration() {
        return iteration;
    }

    @Override
    public boolean isMultiGraph() {
        return isMultiGraph;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public long relationshipCount() {
        return relationshipCount;
    }

    @Override
    public int degree(long nodeId) {
        return degrees.degree(nodeId);
    }

    @Override
    public void voteToHalt(long nodeId) {
        voteBits.set(nodeId);
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        messenger.sendTo(targetNodeId, message);
        hasSendMessage = true;
    }

    @Override
    public void sendToNeighbors(long sourceNodeId, double message) {
        relationshipIterator.forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            sendTo(targetNodeId, message);
            return true;
        });
    }

    @Override
    public void sendToNeighborsWeighted(long sourceNodeId, double message) {
        relationshipIterator.forEachRelationship(sourceNodeId, 1.0, (ignored, targetNodeId, weight) -> {
            sendTo(targetNodeId, computation.applyRelationshipWeight(message, weight));
            return true;
        });
    }

    @Override
    public void forEachNeighbor(long sourceNodeId, LongConsumer targetConsumer) {
        relationshipIterator.forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            targetConsumer.accept(targetNodeId);
            return true;
        });
    }

    @Override
    public void forEachDistinctNeighbor(long sourceNodeId, LongConsumer targetConsumer) {
        var prevTarget = new MutableLong(-1);
        relationshipIterator.forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            if (prevTarget.longValue() != targetNodeId) {
                targetConsumer.accept(targetNodeId);
                prevTarget.setValue(targetNodeId);
            }
            return true;
        });
    }

    @Override
    public double doubleNodeValue(String key, long nodeId) {
        return nodeValue.doubleValue(key, nodeId);
    }

    @Override
    public long longNodeValue(String key, long nodeId) {
        return nodeValue.longValue(key, nodeId);
    }

    @Override
    public long[] longArrayNodeValue(String key, long nodeId) {
        return nodeValue.longArrayValue(key, nodeId);
    }

    @Override
    public double[] doubleArrayNodeValue(String key, long nodeId) {
        return nodeValue.doubleArrayValue(key, nodeId);
    }

    @Override
    public void setNodeValue(String key, long nodeId, double value) {
        nodeValue.set(key, nodeId, value);
    }

    @Override
    public void setNodeValue(String key, long nodeId, long value) {
        nodeValue.set(key, nodeId, value);
    }

    @Override
    public void setNodeValue(String key, long nodeId, long[] value) {
        nodeValue.set(key, nodeId, value);
    }

    @Override
    public void setNodeValue(String key, long nodeId, double[] value) {
        nodeValue.set(key, nodeId, value);
    }
}
