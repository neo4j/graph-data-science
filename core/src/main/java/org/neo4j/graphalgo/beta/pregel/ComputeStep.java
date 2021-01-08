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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.Queue;
import java.util.stream.LongStream;

public final class ComputeStep<CONFIG extends PregelConfig> implements Runnable {

    private final long nodeCount;
    private final long relationshipCount;
    private final boolean isAsync;
    private final boolean isMultiGraph;
    private final PregelComputation<CONFIG> computation;
    private final InitContext<CONFIG> initContext;
    private final ComputeContext<CONFIG> computeContext;
    private final Partition nodeBatch;
    private final Degrees degrees;
    private final Pregel.CompositeNodeValue nodeValues;
    private final HugeObjectArray<? extends Queue<Double>> messageQueues;
    private final RelationshipIterator relationshipIterator;

    private int iteration;
    private HugeAtomicBitSet messageBits;
    private HugeAtomicBitSet prevMessageBits;
    private final HugeAtomicBitSet voteBits;

    ComputeStep(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        int iteration,
        Partition nodeBatch,
        Pregel.CompositeNodeValue nodeValues,
        HugeObjectArray<? extends Queue<Double>> messageQueues,
        HugeAtomicBitSet voteBits,
        RelationshipIterator relationshipIterator
    ) {
        this.iteration = iteration;
        this.nodeCount = graph.nodeCount();
        this.relationshipCount = graph.relationshipCount();
        this.isAsync = config.isAsynchronous();
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.degrees = graph;
        this.isMultiGraph = graph.isMultiGraph();
        this.nodeValues = nodeValues;
        this.messageQueues = messageQueues;
        this.relationshipIterator = relationshipIterator.concurrentCopy();
        this.computeContext = new ComputeContext<>(this, config);
        this.initContext = new InitContext<>(this, config, graph);
    }

    void init(
        int iteration,
        HugeAtomicBitSet messageBits,
        HugeAtomicBitSet prevMessageBits
    ) {
        this.iteration = iteration;
        this.messageBits = messageBits;
        this.prevMessageBits = prevMessageBits;
    }

    @Override
    public void run() {
        var messageIterator = isAsync
            ? new Messages.MessageIterator.Async()
            : new Messages.MessageIterator.Sync();
        var messages = new Messages(messageIterator);

        long batchStart = nodeBatch.startNode();
        long batchEnd = batchStart + nodeBatch.nodeCount();

        for (long nodeId = batchStart; nodeId < batchEnd; nodeId++) {

            if (computeContext.isInitialSuperstep()) {
                initContext.setNodeId(nodeId);
                computation.init(initContext);
            }

            if (prevMessageBits.get(nodeId) || !voteBits.get(nodeId)) {
                voteBits.clear(nodeId);
                computeContext.setNodeId(nodeId);

                messageIterator.init(receiveMessages(nodeId));
                computation.compute(computeContext, messages);
            }
        }
    }

    public int iteration() {
        return iteration;
    }

    public boolean isMultiGraph() {
        return isMultiGraph;
    }

    public long nodeCount() {
        return nodeCount;
    }

    public long relationshipCount() {
        return relationshipCount;
    }

    public int degree(long nodeId) {
        return degrees.degree(nodeId);
    }

    public void voteToHalt(long nodeId) {
        voteBits.set(nodeId);
    }

    public void sendToNeighbors(long sourceNodeId, double message) {
        relationshipIterator.forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            sendTo(targetNodeId, message);
            return true;
        });
    }

    public LongStream getNeighbors(long sourceNodeId) {
        LongStream.Builder builder = LongStream.builder();
        relationshipIterator.forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            builder.accept(targetNodeId);
            return true;
        });
        return builder.build();
    }

    public void sendTo(long targetNodeId, double message) {
        messageQueues.get(targetNodeId).add(message);
        messageBits.set(targetNodeId);
    }

    public void sendToNeighborsWeighted(long sourceNodeId, double message) {
        relationshipIterator.forEachRelationship(sourceNodeId, 1.0, (source, target, weight) -> {
            messageQueues.get(target).add(computation.applyRelationshipWeight(message, weight));
            messageBits.set(target);
            return true;
        });
    }

    private @Nullable Queue<Double> receiveMessages(long nodeId) {
        return prevMessageBits.get(nodeId) ? messageQueues.get(nodeId) : null;
    }

    public double doubleNodeValue(String key, long nodeId) {
        return nodeValues.doubleValue(key, nodeId);
    }

    public long longNodeValue(String key, long nodeId) {
        return nodeValues.longValue(key, nodeId);
    }

    public long[] longArrayNodeValue(String key, long nodeId) {
        return nodeValues.longArrayValue(key, nodeId);
    }

     public double[] doubleArrayNodeValue(String key, long nodeId) {
        return nodeValues.doubleArrayValue(key, nodeId);
    }

    public void setNodeValue(String key, long nodeId, double value) {
        nodeValues.set(key, nodeId, value);
    }

    public void setNodeValue(String key, long nodeId, long value) {
        nodeValues.set(key, nodeId, value);
    }

    public void setNodeValue(String key, long nodeId, long[] value) {
        nodeValues.set(key, nodeId, value);
    }

    public void setNodeValue(String key, long nodeId, double[] value) {
        nodeValues.set(key, nodeId, value);
    }
}
