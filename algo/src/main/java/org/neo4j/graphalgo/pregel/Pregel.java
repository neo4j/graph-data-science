/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.pregel;

import com.carrotsearch.hppc.ArraySizingStrategy;
import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayList;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.huge.loader.HugeNodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.LazyMappingCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

public final class Pregel {

    private final Graph graph;
    private final HugeWeightMapping nodeValues;

    // 1 Use HugeAtomicLongArray
    // * requires combine method
    private final ConcurrentMap<Long, Map<Long, Double>> outgoingMessages;
    private final ConcurrentMap<Long, Map<Long, Double>> incomingMessages;
    private final double[][] messages;


    private final Computation computation;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final ProgressLogger progressLogger;

    private int iterations;

    public static Pregel withDefaultNodeValues(
            final Graph graph,
            final Computation computation,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {

        final HugeWeightMapping nodeValues = HugeNodePropertiesBuilder
                .of(graph.nodeCount(), tracker, computation.getDefaultNodeValue(), 0)
                .build();

        return new Pregel(graph, computation, nodeValues, batchSize, concurrency, executor, tracker, progressLogger);
    }

    public static Pregel withInitialNodeValues(
            final Graph graph,
            final Computation computation,
            final HugeWeightMapping nodeValues,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {

        return new Pregel(graph, computation, nodeValues, batchSize, concurrency, executor, tracker, progressLogger);
    }

    private Pregel(
            final Graph graph,
            final Computation computation,
            final HugeWeightMapping nodeValues,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {
        this.graph = graph;
        this.computation = computation;
        this.tracker = tracker;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.progressLogger = progressLogger;

        this.nodeValues = nodeValues;

        this.messages = new double[(int) graph.nodeCount()][];

        if (computation.getMessageDirection() == Direction.BOTH) {
            outgoingMessages = new ConcurrentHashMap<>((int) graph.nodeCount());
            incomingMessages = new ConcurrentHashMap<>((int) graph.nodeCount());
        } else if (computation.getMessageDirection() == Direction.OUTGOING) {
            outgoingMessages = new ConcurrentHashMap<>((int) graph.nodeCount());
            incomingMessages = null;
        } else {
            outgoingMessages = null;
            incomingMessages = new ConcurrentHashMap<>((int) graph.nodeCount());
        }
    }

    public HugeWeightMapping run(final int maxIterations) {
        iterations = 0;

        boolean canHalt = false;
        while (iterations < maxIterations && !canHalt) {
            int iteration = iterations++;
            final List<ComputeStep> computeSteps = runComputeSteps(iteration);

            // maybe use AtomicBitSet for memory efficiency?
            final BitSet messageBits = computeSteps.get(0).getMessageBits();
            for (int i = 1; i < computeSteps.size(); i++) {
                messageBits.union(computeSteps.get(i).getMessageBits());
            }

            if (messageBits.nextSetBit(0) == -1) {
                canHalt = true;
            } else {
                final List<MessageStep> messageSteps = runMessageSteps(messageBits);
            }
        }
        return nodeValues;
    }

    public int getIterations() {
        return iterations;
    }

    private List<ComputeStep> runComputeSteps(final int iteration) {
        Collection<PrimitiveLongIterable> iterables = graph.batchIterables(batchSize);

        int threads = iterables.size();

        final List<ComputeStep> tasks = new ArrayList<>(threads);

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                iterables,
                nodeIterable -> {
                    ComputeStep task = new ComputeStep(
                            computation,
                            graph.nodeCount(),
                            iteration,
                            messages,
                            nodeIterable,
                            graph,
                            nodeValues,
                            outgoingMessages,
                            incomingMessages,
                            graph);
                    tasks.add(task);
                    return task;
                });

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        return tasks;
    }

    private List<MessageStep> runMessageSteps(final BitSet messageBits) {
        Collection<PrimitiveLongIterable> iterables = graph.batchIterables(batchSize);

        int threads = iterables.size();

        final List<MessageStep> tasks = new ArrayList<>(threads);

        Collection<MessageStep> computeSteps = LazyMappingCollection.of(
                iterables,
                nodeIterable -> {
                    MessageStep task = new MessageStep(
                            messageBits,
                            messages,
                            computation.getMessageDirection(),
                            nodeIterable,
                            graph,
                            outgoingMessages,
                            incomingMessages,
                            graph);
                    tasks.add(task);
                    return task;
                });

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        return tasks;
    }

    public static final class ComputeStep implements Runnable {

        private final int iteration;
        private final Computation computation;
        private final BitSet messageBits;
        private final double[][] messages;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeWeightMapping nodeProperties;
        private final ConcurrentMap<Long, Map<Long, Double>> outgoingMessages;
        private final ConcurrentMap<Long, Map<Long, Double>> incomingMessages;
        private final RelationshipIterator relationshipIterator;

        private ComputeStep(
                final Computation computation,
                final long totalNodeCount,
                final int iteration,
                final double[][] messages,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeWeightMapping nodeProperties,
                final ConcurrentMap<Long, Map<Long, Double>> outgoingMessages,
                final ConcurrentMap<Long, Map<Long, Double>> incomingMessages,
                final RelationshipIterator relationshipIterator) {
            this.iteration = iteration;
            this.computation = computation;
            this.messageBits = new BitSet(totalNodeCount);
            this.messages = messages;
            this.nodes = nodes;
            this.degrees = degrees;
            this.nodeProperties = nodeProperties;
            this.outgoingMessages = outgoingMessages;
            this.incomingMessages = incomingMessages;
            this.relationshipIterator = relationshipIterator.concurrentCopy();

            computation.setComputeStep(this);
        }

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodes.iterator();

            while (nodesIterator.hasNext()) {
                final long nodeId = nodesIterator.next();
                computation.compute(nodeId, messages[(int) nodeId]);
            }
        }

        BitSet getMessageBits() {
            return messageBits;
        }

        public int getIteration() {
            return iteration;
        }

        int getDegree(final long nodeId, Direction direction) {
            return degrees.degree(nodeId, direction);
        }

        double getNodeValue(final long nodeId) {
            return nodeProperties.nodeWeight(nodeId);
        }

        void setNodeValue(final long nodeId, final double value) {
            nodeProperties.putNodeWeight(nodeId, value);
        }

        synchronized void sendMessages(final long nodeId, final double message, Direction direction) {
            if (direction == Direction.OUTGOING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                    outgoingMessages
                            .computeIfAbsent(sourceNodeId, k -> new ConcurrentHashMap<>())
                            .put(targetNodeId, message);
                    messageBits.set(targetNodeId);
                    return true;
                });
            }

            if (direction == Direction.INCOMING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(nodeId, Direction.INCOMING, (targetNodeId, sourceNodeId) -> {
                    incomingMessages
                            .computeIfAbsent(sourceNodeId, k -> new ConcurrentHashMap<>())
                            .put(targetNodeId, message);
                    messageBits.set(sourceNodeId);
                    return true;
                });
            }
        }
    }

    public static final class MessageStep implements Runnable {
        private final BitSet messageBits;
        private final double[][] messages;
        private final Direction messageDirection;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final ConcurrentMap<Long, Map<Long, Double>> outgoingMessages;
        private final ConcurrentMap<Long, Map<Long, Double>> incomingMessages;
        private final RelationshipIterator relationshipIterator;

        private static final double[] NO_MESSAGES = new double[0];

        private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
                (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

        MessageStep(
                final BitSet messageBits,
                final double[][] messages,
                final Direction messageDirection,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final ConcurrentMap<Long, Map<Long, Double>> outgoingMessages,
                final ConcurrentMap<Long, Map<Long, Double>> incomingMessages,
                final RelationshipIterator relationshipIterator) {
            this.messageBits = messageBits;
            this.messages = messages;
            this.messageDirection = messageDirection.reverse();
            this.nodes = nodes;
            this.degrees = degrees;
            this.outgoingMessages = outgoingMessages;
            this.incomingMessages = incomingMessages;
            this.relationshipIterator = relationshipIterator.concurrentCopy();
        }

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodes.iterator();

            while (nodesIterator.hasNext()) {
                final long nodeId = nodesIterator.next();
                messages[(int) nodeId] = receiveMessages(nodeId, messageDirection, messages[(int) nodeId]);
            }
        }

        private double[] receiveMessages(final long nodeId, Direction direction, double[] messages) {
            if (!messageBits.get(nodeId)) {
                return NO_MESSAGES;
            }
            final int degree = degrees.degree(nodeId, direction);
            final DoubleArrayList doubleCursors;

            if (messages != null) {
                doubleCursors = new DoubleArrayList(0, ARRAY_SIZING_STRATEGY);
                doubleCursors.buffer = messages;
            } else {
                doubleCursors = new DoubleArrayList(degree, ARRAY_SIZING_STRATEGY);
            }

            if (direction == Direction.INCOMING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(
                        nodeId,
                        Direction.INCOMING,
                        (sourceNodeId, targetNodeId) -> {
                            Map<Long, Double> localMessages = outgoingMessages.get(targetNodeId);
                            if (localMessages != null) {
                                Double message = localMessages.get(sourceNodeId);
                                if (message != null) {
                                    doubleCursors.add(message);
                                }
                            }
                            return true;
                        });
            }

            if (direction == Direction.OUTGOING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(
                        nodeId,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            Map<Long, Double> localMessages = incomingMessages.get(sourceNodeId);
                            if (localMessages != null) {
                                Double message = localMessages.get(targetNodeId);
                                if (message != null) {
                                    doubleCursors.add(message);
                                }
                            }
                            return true;
                        });
            }

            assert (doubleCursors.elementsCount == degree);

            return doubleCursors.buffer;
        }
    }
}
