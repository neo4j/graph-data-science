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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.LongConsumer;

public interface ComputeStep<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator> {

    Graph graph();

    HugeAtomicBitSet voteBits();

    PregelComputation<CONFIG> computation();

    NodeValue nodeValue();

    Messenger<ITERATOR> messenger();

    Partition nodeBatch();

    InitContext<CONFIG> initContext();

    ComputeContext<CONFIG> computeContext();

    ProgressTracker progressTracker();

    int iteration();

    default boolean isMultiGraph() {
        return graph().isMultiGraph();
    }

    default long nodeCount() {
        return graph().nodeCount();
    }

    default long relationshipCount() {
        return graph().relationshipCount();
    }

    default int degree(long nodeId) {
        return graph().degree(nodeId);
    }

    default void voteToHalt(long nodeId) {
        voteBits().set(nodeId);
    }

    void sendTo(long targetNodeId, double message);

    default void computeBatch() {
        var messenger = messenger();
        var messageIterator = messenger.messageIterator();
        var messages = new Messages(messageIterator);

        var nodeBatch = nodeBatch();
        var computation = computation();
        var initContext = initContext();
        var computeContext = computeContext();
        var voteBits = voteBits();

        nodeBatch.consume(nodeId -> {
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
        });
        progressTracker().logProgress(nodeBatch.nodeCount());
    }

    default void sendToNeighbors(long sourceNodeId, double message) {
        graph().forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            sendTo(targetNodeId, message);
            return true;
        });
    }

    default void sendToNeighborsWeighted(long sourceNodeId, double message) {
        graph().forEachRelationship(sourceNodeId, 1.0, (ignored, targetNodeId, weight) -> {
            sendTo(targetNodeId, computation().applyRelationshipWeight(message, weight));
            return true;
        });
    }

    default void forEachNeighbor(long sourceNodeId, LongConsumer targetConsumer) {
        graph().forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            targetConsumer.accept(targetNodeId);
            return true;
        });
    }

    default void forEachDistinctNeighbor(long sourceNodeId, LongConsumer targetConsumer) {
        var prevTarget = new MutableLong(-1);
        graph().forEachRelationship(sourceNodeId, (ignored, targetNodeId) -> {
            if (prevTarget.longValue() != targetNodeId) {
                targetConsumer.accept(targetNodeId);
                prevTarget.setValue(targetNodeId);
            }
            return true;
        });
    }

    default double doubleNodeValue(String key, long nodeId) {
        return nodeValue().doubleValue(key, nodeId);
    }

    default long longNodeValue(String key, long nodeId) {
        return nodeValue().longValue(key, nodeId);
    }

    default long[] longArrayNodeValue(String key, long nodeId) {
        return nodeValue().longArrayValue(key, nodeId);
    }

    default double[] doubleArrayNodeValue(String key, long nodeId) {
        return nodeValue().doubleArrayValue(key, nodeId);
    }

    default void setNodeValue(String key, long nodeId, double value) {
        nodeValue().set(key, nodeId, value);
    }

    default void setNodeValue(String key, long nodeId, long value) {
        nodeValue().set(key, nodeId, value);
    }

    default void setNodeValue(String key, long nodeId, long[] value) {
        nodeValue().set(key, nodeId, value);
    }

    default void setNodeValue(String key, long nodeId, double[] value) {
        nodeValue().set(key, nodeId, value);
    }
}
