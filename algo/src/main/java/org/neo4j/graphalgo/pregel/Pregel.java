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
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongDoubleMap;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class Pregel {

    private final Graph graph;
    private final HugeWeightMapping nodeValues;
    private final HugeLongLongDoubleMap outgoingMessages;
    private final HugeLongLongDoubleMap incomingMessages;
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

        if (computation.getMessageDirection() == Direction.BOTH) {
            outgoingMessages = new HugeLongLongDoubleMap(graph.relationshipCount(), tracker);
            incomingMessages = new HugeLongLongDoubleMap(graph.relationshipCount(), tracker);
        } else if (computation.getMessageDirection() == Direction.OUTGOING) {
            outgoingMessages = new HugeLongLongDoubleMap(graph.relationshipCount(), tracker);
            incomingMessages = null;
        } else {
            outgoingMessages = null;
            incomingMessages = new HugeLongLongDoubleMap(graph.relationshipCount(), tracker);
        }
    }

    public HugeWeightMapping run(final int maxIterations) {
        iterations = 0;
        BitSet messages = new BitSet(graph.nodeCount());

        boolean canHalt = false;
        while (iterations < maxIterations && !canHalt) {
            final List<ComputeStep> computeSteps = runSuperstep(iterations++, messages);
            canHalt = (computeSteps.parallelStream().map(ComputeStep::canHalt).reduce(true, (l, r) -> l && r));
        }
        return nodeValues;
    }

    public int getIterations() {
        return iterations;
    }

    private List<ComputeStep> runSuperstep(final int iteration, final BitSet messages) {
        Collection<PrimitiveLongIterable> iterators = graph.batchIterables(batchSize);

        int threads = iterators.size();

        final List<ComputeStep> tasks = new ArrayList<>(threads);

        Collection<ComputeStep> computeSteps = LazyMappingCollection.of(
                iterators,
                nodeIterator -> {
                    ComputeStep task = new ComputeStep(
                            computation,
                            messages,
                            iteration,
                            nodeIterator,
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

    public static final class ComputeStep implements Runnable {

        private final int iteration;
        private final Computation computation;
        private final BitSet messages;
        private final PrimitiveLongIterable nodes;
        private final Degrees degrees;
        private final HugeWeightMapping nodeProperties;
        private final HugeLongLongDoubleMap outgoingMessages;
        private final HugeLongLongDoubleMap incomingMessages;
        private final RelationshipIterator relationshipIterator;

        private ComputeStep(
                final Computation computation,
                final BitSet messages,
                final int iteration,
                final PrimitiveLongIterable nodes,
                final Degrees degrees,
                final HugeWeightMapping nodeProperties,
                final HugeLongLongDoubleMap outgoingMessages,
                final HugeLongLongDoubleMap incomingMessages,
                final RelationshipIterator relationshipIterator) {
            this.iteration = iteration;
            this.computation = computation;
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
                computation.compute(nodesIterator.next());
            }
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

        private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
                (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

        private static final double[] NO_MESSAGES = new double[0];

        double[] receiveMessages(final long nodeId, Direction direction) {
            if (!messages.get(nodeId)) {
                return NO_MESSAGES;
            }
            messages.flip(nodeId);
            final int degree = degrees.degree(nodeId, direction);
            final DoubleArrayList doubleCursors = new DoubleArrayList(degree, ARRAY_SIZING_STRATEGY);

            if (direction == Direction.INCOMING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(
                        nodeId,
                        Direction.INCOMING,
                        (sourceNodeId, targetNodeId) -> {
                            doubleCursors.add(outgoingMessages.getOrDefault(targetNodeId, sourceNodeId, 1.0));
                            return true;
                        });
            }

            if (direction == Direction.OUTGOING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(
                        nodeId,
                        Direction.OUTGOING,
                        (sourceNodeId, targetNodeId) -> {
                            doubleCursors.add(incomingMessages.getOrDefault(sourceNodeId, targetNodeId, 1.0));
                            return true;
                        });
            }

            assert (doubleCursors.buffer.length == degree);
            assert (doubleCursors.elementsCount == degree);

            return doubleCursors.buffer;
        }

        void sendMessages(final long nodeId, final double message, Direction direction) {
            if (direction == Direction.OUTGOING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                    outgoingMessages.set(sourceNodeId, targetNodeId, message);
                    messages.set(targetNodeId);
                    return true;
                });
            }

            if (direction == Direction.INCOMING || direction == Direction.BOTH) {
                relationshipIterator.forEachRelationship(nodeId, Direction.INCOMING, (targetNodeId, sourceNodeId) -> {
                    incomingMessages.set(sourceNodeId, targetNodeId, message);
                    messages.set(sourceNodeId);
                    return true;
                });
            }
        }

        boolean canHalt() {
            boolean canHalt = true;
            final PrimitiveLongIterator nodes = this.nodes.iterator();
            while (nodes.hasNext()) {
                if (messages.get(nodes.next())) {
                    canHalt = false;
                    break;
                }
            }
            return canHalt;
        }
    }
}
