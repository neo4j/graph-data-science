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
import org.jctools.queues.MpscLinkedQueue;
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
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public final class Pregel {

    private final Graph graph;

    private final HugeDoubleArray nodeValues;
    private final MpscLinkedQueue<Double>[] messageQueues;
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
            final HugeWeightMapping nodeProperties,
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

        this.nodeValues = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> nodeValues.set(nodeId, nodeProperties.nodeWeight(nodeId)))
        );

        this.messages = new double[(int) graph.nodeCount()][];
        this.messageQueues = new MpscLinkedQueue[(int) graph.nodeCount()];
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> messageQueues[(int) nodeId] = MpscLinkedQueue.newMpscLinkedQueue()));
    }

    public HugeDoubleArray run(final int maxIterations) {
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
                runMessageSteps(messageBits);
            }
        }
        return nodeValues;
    }

    public int getIterations() {
        return iterations;
    }

    private List<ComputeStep> runComputeSteps(final int iteration) {
        Collection<PrimitiveLongIterable> iterables = graph.batchIterables(batchSize);

        int threadCount = iterables.size();

        final List<ComputeStep> tasks = new ArrayList<>(threadCount);

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
                            messageQueues,
                            graph);
                    tasks.add(task);
                    return task;
                });

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
        return tasks;
    }

    private void runMessageSteps(final BitSet messageBits) {
        Collection<PrimitiveLongIterable> iterables = graph.batchIterables(batchSize);

        Collection<MessageStep> computeSteps = LazyMappingCollection.of(
                iterables,
                nodeIterable -> new MessageStep(
                        messageBits,
                        messages,
                        computation.getMessageDirection(),
                        nodeIterable,
                        graph,
                        messageQueues
                ));

        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
    }

    public static final class ComputeStep implements Runnable {

        private final int iteration;
        private final Computation computation;
        private final BitSet messageBits;
        private final double[][] messages;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeDoubleArray nodeProperties;
        private final MpscLinkedQueue<Double>[] messageQueues;
        private final ThreadLocal<RelationshipIterator> relationshipIterator;

        private ComputeStep(
                final Computation computation,
                final long totalNodeCount,
                final int iteration,
                final double[][] messages,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeDoubleArray nodeProperties,
                final MpscLinkedQueue<Double>[] messageQueues,
                final RelationshipIterator relationshipIterator) {
            this.iteration = iteration;
            this.computation = computation;
            this.messageBits = new BitSet(totalNodeCount);
            this.messages = messages;
            this.nodes = nodes;
            this.degrees = degrees;
            this.nodeProperties = nodeProperties;
            this.messageQueues = messageQueues;
            this.relationshipIterator = ThreadLocal.withInitial(relationshipIterator::concurrentCopy);

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
            return nodeProperties.get(nodeId);
        }

        void setNodeValue(final long nodeId, final double value) {
            nodeProperties.set(nodeId, value);
        }

        void sendMessages(final long nodeId, final double message, Direction direction) {
            relationshipIterator.get().forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId) -> {
                messageQueues[(int) targetNodeId].add(message);
                messageBits.set(targetNodeId);
                return true;
            });
        }
    }

    public static final class MessageStep implements Runnable {
        private final BitSet messageBits;
        private final double[][] messages;
        private final Direction messageDirection;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final MpscLinkedQueue<Double>[] messageQueues;

        private static final double[] NO_MESSAGES = new double[0];

        private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
                (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

        MessageStep(
                final BitSet messageBits,
                final double[][] messages,
                final Direction messageDirection,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final MpscLinkedQueue<Double>[] messageQueues) {
            this.messageBits = messageBits;
            this.messages = messages;
            this.messageDirection = messageDirection.reverse();
            this.nodes = nodes;
            this.degrees = degrees;
            this.messageQueues = messageQueues;
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

            Double nextMessage;

            while ((nextMessage = messageQueues[(int) nodeId].poll()) != null) {
                doubleCursors.add(nextMessage);
            }

            return doubleCursors.buffer;
        }
    }
}
