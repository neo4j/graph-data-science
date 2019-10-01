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
// TODO: move to `beta` package namespace
package org.neo4j.graphalgo.pregel;

import com.carrotsearch.hppc.BitSet;
import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.huge.loader.NodePropertiesBuilder;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.LazyMappingCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

public final class Pregel {

    // Marks the end of messages from the previous iteration
    private static final Double TERMINATION_SYMBOL = Double.NaN;

    private final Supplier<Computation> computationFactory;

    private final Graph graph;

    private final HugeDoubleArray nodeValues;
    // TODO: Compare performance to pre-sized MpscArrayQueue
    private final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues;

    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final ProgressLogger progressLogger;

    private int iterations;

    public static Pregel withDefaultNodeValues(
            final Graph graph,
            final Supplier<Computation> computationFactory,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {

        final NodeWeights nodeValues = NodePropertiesBuilder
                .of(graph.nodeCount(), tracker, computationFactory.get().getDefaultNodeValue(), 0, "key")
                .build();

        return new Pregel(
                graph,
                computationFactory,
                nodeValues,
                batchSize,
                concurrency,
                executor,
                tracker,
                progressLogger);
    }

    public static Pregel withInitialNodeValues(
            final Graph graph,
            final Supplier<Computation> computationFactory,
            final NodeWeights initialNodeValues,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {

        return new Pregel(
                graph,
                computationFactory,
                initialNodeValues,
                batchSize,
                concurrency,
                executor,
                tracker,
                progressLogger);
    }

    private Pregel(
            final Graph graph,
            final Supplier<Computation> computationFactory,
            final NodeWeights initialNodeValues,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {
        this.graph = graph;
        this.computationFactory = computationFactory;
        this.tracker = tracker;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.progressLogger = progressLogger;

        // HugeDoubleArray is faster for set operations compared to HugeNodePropertyMap
        this.nodeValues = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> nodeValues.set(nodeId, initialNodeValues.nodeWeight(nodeId)))
        );

        MpscLinkedQueue<Double> tempQueue = MpscLinkedQueue.newMpscLinkedQueue();
        Class<MpscLinkedQueue<Double>> queueClass = (Class<MpscLinkedQueue<Double>>) tempQueue.getClass();
        this.messageQueues = HugeObjectArray.newArray(queueClass, graph.nodeCount(), tracker);
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> messageQueues.set(nodeId, MpscLinkedQueue.newMpscLinkedQueue())));
    }

    public HugeDoubleArray run(final int maxIterations) {
        iterations = 0;
        boolean canHalt = false;
        // Tracks if a node received messages in the previous iteration
        // TODO: try RoaringBitSet instead
        BitSet receiverBits = new BitSet(graph.nodeCount());
        // Tracks if a node voted to halt in the previous iteration
        BitSet voteBits = new BitSet(graph.nodeCount());

        while (iterations < maxIterations && !canHalt) {
            int iteration = iterations++;

            final List<ComputeStep> computeSteps = runComputeSteps(iteration, receiverBits, voteBits);

            receiverBits = unionBitSets(computeSteps, ComputeStep::getSenders);
            voteBits = unionBitSets(computeSteps, ComputeStep::getVotes);

            // No messages have been sent
            if (receiverBits.nextSetBit(0) == -1) {
                canHalt = true;
            }
        }
        return nodeValues;
    }

    public int getIterations() {
        return iterations;
    }

    private BitSet unionBitSets(List<ComputeStep> computeSteps, Function<ComputeStep, BitSet> fn) {
        BitSet target = fn.apply(computeSteps.get(0));
        for (int i = 1; i < computeSteps.size(); i++) {
            target.union(fn.apply(computeSteps.get(i)));
        }
        return target;
    }

    private List<ComputeStep> runComputeSteps(
            final int iteration,
            BitSet messageBits,
            BitSet voteToHaltBits) {
        // TODO: maybe try degree partitioning or clustering (better locality)
        // TODO: can be initialized outside of iteration
        Collection<PrimitiveLongIterable> nodeBatches = LazyBatchCollection.of(
                graph.nodeCount(),
                batchSize,
                (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L));

        int threadCount = nodeBatches.size();

        final List<ComputeStep> tasks = new ArrayList<>(threadCount);

        if (!computationFactory.get().supportsAsynchronousParallel()) {
            // Synchronization barrier:
            // Add termination flag to message queues that
            // received messages in the previous iteration.
            if (iteration > 0) {
                ParallelUtil.parallelStreamConsume(
                        LongStream.range(0, graph.nodeCount()),
                        nodeIds -> nodeIds.forEach(nodeId -> {
                            if (messageBits.get(nodeId)) {
                                messageQueues.get(nodeId).add(TERMINATION_SYMBOL);
                            }
                        }));
            }
        }

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                nodeBatches,
                nodeBatch -> {
                    ComputeStep task = new ComputeStep(
                            computationFactory.get(),
                            graph.nodeCount(),
                            iteration,
                            nodeBatch,
                            graph,
                            nodeValues,
                            messageBits,
                            voteToHaltBits,
                            messageQueues,
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
        private final BitSet senderBits;
        private final BitSet receiverBits;
        private final BitSet voteBits;
        private final PrimitiveLongIterable nodeBatch;
        private final Degrees degrees;
        private final HugeDoubleArray nodeValues;
        private final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues;
        private final RelationshipIterator relationshipIterator;

        private ComputeStep(
                final Computation computation,
                final long globalNodeCount,
                final int iteration,
                final PrimitiveLongIterable nodeBatch,
                final Degrees degrees,
                final HugeDoubleArray nodeValues,
                final BitSet receiverBits,
                final BitSet voteBits,
                final HugeObjectArray<MpscLinkedQueue<Double>> messageQueues,
                final RelationshipIterator relationshipIterator) {
            this.iteration = iteration;
            this.computation = computation;
            this.senderBits = new BitSet(globalNodeCount);
            this.receiverBits = receiverBits;
            this.voteBits = voteBits;
            this.nodeBatch = nodeBatch;
            this.degrees = degrees;
            this.nodeValues = nodeValues;
            this.messageQueues = messageQueues;
            this.relationshipIterator = relationshipIterator.concurrentCopy();

            computation.setComputeStep(this);
        }

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodeBatch.iterator();

            while (nodesIterator.hasNext()) {
                final long nodeId = nodesIterator.next();

                if (receiverBits.get(nodeId) || !voteBits.get(nodeId)) {
                    voteBits.clear(nodeId);
                    computation.compute(nodeId, receiveMessages(nodeId));
                }
            }
        }

        BitSet getSenders() {
            return senderBits;
        }

        BitSet getVotes() {
            return voteBits;
        }

        public int getIteration() {
            return iteration;
        }

        int getDegree(final long nodeId, Direction direction) {
            return degrees.degree(nodeId, direction);
        }

        double getNodeValue(final long nodeId) {
            return nodeValues.get(nodeId);
        }

        void setNodeValue(final long nodeId, final double value) {
            nodeValues.set(nodeId, value);
        }

        void voteToHalt(long nodeId) {
            voteBits.set(nodeId);
        }

        void sendMessages(final long nodeId, final double message, Direction direction) {
            relationshipIterator.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId) -> {
                messageQueues.get(targetNodeId).add(message);
                senderBits.set(targetNodeId);
                return true;
            });
        }

        private MpscLinkedQueue<Double> receiveMessages(final long nodeId) {
            return receiverBits.get(nodeId) ? messageQueues.get(nodeId) : null;
        }
    }
}
