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

import com.carrotsearch.hppc.BitSet;
import org.jctools.queues.MpscLinkedQueue;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeNodeWeights;
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
import java.util.function.Supplier;
import java.util.stream.LongStream;

public final class Pregel {

    private final Graph graph;

    private final HugeDoubleArray nodeValues;
    private final MpscLinkedQueue<Double>[] primaryQueues;
    private final MpscLinkedQueue<Double>[] secondaryQueues;

    private final Supplier<Computation> computationFactory;
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

        final HugeNodeWeights nodeValues = HugeNodePropertiesBuilder
                .of(graph.nodeCount(), tracker, computationFactory.get().getDefaultNodeValue(), 0)
                .build();

        return new Pregel(graph, computationFactory, nodeValues, batchSize, concurrency, executor, tracker, progressLogger);
    }

    public static Pregel withInitialNodeValues(
            final Graph graph,
            final Supplier<Computation> computationFactory,
            final HugeNodeWeights nodeValues,
            final int batchSize,
            final int concurrency,
            final ExecutorService executor,
            final AllocationTracker tracker,
            final ProgressLogger progressLogger) {

        return new Pregel(graph, computationFactory, nodeValues, batchSize, concurrency, executor, tracker, progressLogger);
    }

    private Pregel(
            final Graph graph,
            final Supplier<Computation> computationFactory,
            final HugeNodeWeights nodeProperties,
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

        this.nodeValues = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> nodeValues.set(nodeId, nodeProperties.nodeWeight(nodeId)))
        );

        this.primaryQueues = new MpscLinkedQueue[(int) graph.nodeCount()];
        this.secondaryQueues = new MpscLinkedQueue[(int) graph.nodeCount()];
        ParallelUtil.parallelStreamConsume(
                LongStream.range(0, graph.nodeCount()),
                nodeIds -> nodeIds.forEach(nodeId -> {
                    primaryQueues[(int) nodeId] = MpscLinkedQueue.newMpscLinkedQueue();
                    secondaryQueues[(int) nodeId] = MpscLinkedQueue.newMpscLinkedQueue();
                }));
    }

    public HugeDoubleArray run(final int maxIterations) {
        iterations = 0;

        boolean canHalt = false;
        BitSet messageBits = new BitSet(graph.nodeCount());

        while (iterations < maxIterations && !canHalt) {
            int iteration = iterations++;

            final List<ComputeStep> computeSteps = runComputeSteps(iteration, messageBits);

            // maybe use AtomicBitSet for memory efficiency?
            messageBits = computeSteps.get(0).getSenderBits();
            for (int i = 1; i < computeSteps.size(); i++) {
                messageBits.union(computeSteps.get(i).getSenderBits());
            }

            if (messageBits.nextSetBit(0) == -1) {
                canHalt = true;
            }
        }
        return nodeValues;
    }

    public int getIterations() {
        return iterations;
    }

    private List<ComputeStep> runComputeSteps(final int iteration, BitSet messageBits) {
        Collection<PrimitiveLongIterable> iterables = graph.batchIterables(batchSize);

        int threadCount = iterables.size();

        final List<ComputeStep> tasks = new ArrayList<>(threadCount);

        MpscLinkedQueue<Double>[] senderQueues = (iteration % 2 == 0) ? primaryQueues : secondaryQueues;
        MpscLinkedQueue<Double>[] receiverQueues = (iteration % 2 == 0) ? secondaryQueues : primaryQueues;

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                iterables,
                nodeIterable -> {
                    ComputeStep task = new ComputeStep(
                            computationFactory.get(),
                            graph.nodeCount(),
                            iteration,
                            nodeIterable,
                            graph,
                            nodeValues,
                            messageBits,
                            senderQueues,
                            receiverQueues,
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
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeDoubleArray nodeProperties;
        private final MpscLinkedQueue<Double>[] senderQueues;
        private final MpscLinkedQueue<Double>[] receiverQueues;
        private final RelationshipIterator relationshipIterator;

        private ComputeStep(
                final Computation computation,
                final long totalNodeCount,
                final int iteration,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeDoubleArray nodeProperties,
                final BitSet receiverBits,
                final MpscLinkedQueue<Double>[] senderQueues,
                final MpscLinkedQueue<Double>[] receiverQueues,
                final RelationshipIterator relationshipIterator) {
            this.iteration = iteration;
            this.computation = computation;
            this.senderBits = new BitSet(totalNodeCount);
            this.receiverBits = receiverBits;
            this.nodes = nodes;
            this.degrees = degrees;
            this.nodeProperties = nodeProperties;
            this.senderQueues = senderQueues;
            this.receiverQueues = receiverQueues;
            this.relationshipIterator = relationshipIterator.concurrentCopy();

            computation.setComputeStep(this);
        }

        private static final MpscLinkedQueue<Double> EMPTY_QUEUE = MpscLinkedQueue.newMpscLinkedQueue();

        @Override
        public void run() {
            final PrimitiveLongIterator nodesIterator = nodes.iterator();

            while (nodesIterator.hasNext()) {
                final long nodeId = nodesIterator.next();
                if (receiverBits.get(nodeId)) {
                    computation.compute(nodeId, receiverQueues[(int) nodeId]);
                } else {
                    computation.compute(nodeId, EMPTY_QUEUE);
                }
            }
        }

        BitSet getSenderBits() {
            return senderBits;
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
            relationshipIterator.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId) -> {
                senderQueues[(int) targetNodeId].add(message);
                senderBits.set(targetNodeId);
                return true;
            });
        }
    }
}
