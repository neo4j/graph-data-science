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
package org.neo4j.graphalgo.impl.nn;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdMapBuilder;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.NodeImporter;
import org.neo4j.graphalgo.core.loading.NodesBatchBuffer;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.AnnTopKConsumer;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ApproxNearestNeighbors<T extends SimilarityInput> extends Algorithm<ApproxNearestNeighbors<T>> {
    private T[] inputs;
    private final int topK;
    private final int iterations;
    private final AnnTopKConsumer[] topKConsumers;
    private final double similarityCutoff;
    private final Log log;
    private final Supplier<RleDecoder> rleDecoderFactory;
    private final Random random;
    private final AtomicInteger actualIterations;
    private volatile AtomicLong nodeQueue = new AtomicLong();
    private final int concurrency;
    private final ExecutorService executor;
    private final SimilarityComputer<T> similarityComputer;
    private final double precision;
    private final double p;
    private final boolean sampling;
    private final RoaringBitmap[] visitedRelationships;

    public ApproxNearestNeighbors(
            final ProcedureConfiguration configuration,
            final T[] inputs,
            final double similarityCutoff,
            final Supplier<RleDecoder> rleDecoderFactory,
            final SimilarityComputer<T> similarityComputer,
            int topK,
            Log log) {
        this.inputs = inputs;
        this.topK = topK;
        this.iterations = configuration.getNumber("iterations", 10).intValue();
        this.precision = configuration.getNumber("precision", 0.001).doubleValue();
        this.p = configuration.getNumber("p", 0.5).doubleValue();
        this.random = new Random(configuration.getNumber("randomSeed", 1).longValue());
        this.sampling = configuration.getBool("sampling", true);

        this.topKConsumers = AnnTopKConsumer.initializeTopKConsumers(inputs.length, topK);

        this.visitedRelationships = ANNUtils.initializeRoaringBitmaps(inputs.length);

        this.similarityCutoff = similarityCutoff;
        this.log = log;
        this.actualIterations = new AtomicInteger();
        this.concurrency = configuration.getConcurrency();
        this.executor = Pools.DEFAULT;
        this.rleDecoderFactory = rleDecoderFactory;
        this.similarityComputer = similarityComputer;
    }

    public void compute() {
        double sampleSize = Math.min(this.p, 1.0) * Math.abs(this.topK);
        Collection<Runnable> tasks = createInitTasks();
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        IdsAndProperties nodes = buildNodes(inputs);

        RoaringBitmap[] tempVisitedRelationships = ANNUtils.initializeRoaringBitmaps(inputs.length);

        for (int iteration = 1; iteration <= iterations; iteration++) {
            for (int i = 0; i < inputs.length; i++) {
                visitedRelationships[i] = RoaringBitmap.or(visitedRelationships[i], tempVisitedRelationships[i]);
            }
            tempVisitedRelationships = ANNUtils.initializeRoaringBitmaps(inputs.length);

            HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer relationshipBuilder = new HugeRelationshipsBuilder(nodes).withBuffer();
            relationshipBuilder.addRelationshipsFrom(topKConsumers);
            Relationships hugeRels = relationshipBuilder.build();

            HugeGraph hugeGraph = ANNUtils.hugeGraph(nodes, hugeRels);
            HugeRelationshipsBuilder oldRelationshipsBuilder = new HugeRelationshipsBuilder(nodes);
            HugeRelationshipsBuilder newRelationshipBuilder = new HugeRelationshipsBuilder(nodes);

            Collection<Runnable> setupTasks = setupTasks(sampleSize, tempVisitedRelationships, hugeGraph, oldRelationshipsBuilder, newRelationshipBuilder);
            ParallelUtil.runWithConcurrency(1, setupTasks, executor);

            HugeGraph oldHugeGraph = ANNUtils.hugeGraph(nodes, oldRelationshipsBuilder.build());
            HugeGraph newHugeGraph = ANNUtils.hugeGraph(nodes, newRelationshipBuilder.build());

            Collection<NeighborhoodTask> computeTasks = computeTasks(sampleSize, oldHugeGraph, newHugeGraph);
            ParallelUtil.runWithConcurrency(concurrency, computeTasks, executor);

            int changes = mergeConsumers(computeTasks);

            log.info("ANN: Changes in iteration %d: %d", iteration, changes);
            actualIterations.set(iteration);

            if (shouldTerminate(changes)) {
                break;
            }

        }
    }

    private Collection<Runnable> setupTasks(
            double sampleSize,
            RoaringBitmap[] tempVisitedRelationships,
            HugeGraph hugeGraph,
            HugeRelationshipsBuilder oldRelationshipsBuilder,
            HugeRelationshipsBuilder newRelationshipBuilder) {
        int batchSize = ParallelUtil.adjustedBatchSize(inputs.length, concurrency, 100);
        int numberOfBatches = (inputs.length / batchSize) + 1;
        Collection<Runnable> setupTasks = new ArrayList<>(numberOfBatches);

        long startNodeId = 0L;
        for (int batch = 0; batch < numberOfBatches; batch++) {
            long nodeCount = Math.min(batchSize, inputs.length - (batch * batchSize));
            setupTasks.add(new SetupTask(
                    new NewOldGraph(hugeGraph, visitedRelationships),
                    tempVisitedRelationships,
                    oldRelationshipsBuilder,
                    newRelationshipBuilder,
                    sampleSize,
                    startNodeId,
                    nodeCount));
            startNodeId += nodeCount;
        }
        return setupTasks;
    }

    private Collection<NeighborhoodTask> computeTasks(double sampleSize, HugeGraph oldHugeGraph, HugeGraph newHugeGraph) {
        nodeQueue.set(0);
        Collection<NeighborhoodTask> computeTasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            computeTasks.add(new ComputeTask(
                    rleDecoderFactory, inputs.length,
                    oldHugeGraph,
                    newHugeGraph,
                    sampleSize));
        }
        return computeTasks;
    }

    private IdsAndProperties buildNodes(final T[] inputs) {
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(inputs.length, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);
        long maxNodeId = 0L;

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), inputs.length, false);

        for (T input : inputs) {
            if (input.getId() > maxNodeId) {
                maxNodeId = input.getId();
            }
            buffer.add(input.getId(), -1);
            if (buffer.isFull()) {
                nodeImporter.importNodes(buffer, null);
                buffer.reset();
            }
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, maxNodeId, 1, AllocationTracker.EMPTY);
        return new IdsAndProperties(idMap, Collections.emptyMap());
    }

    private List<Runnable> createInitTasks() {
        nodeQueue.set(0);
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new InitTask(rleDecoderFactory));
        }
        return tasks;
    }

    private int mergeConsumers(final Iterable<NeighborhoodTask> neighborhoodTasks) {
        int changes = 0;
        for (NeighborhoodTask task : neighborhoodTasks) {
            changes += task.mergeInto(topKConsumers);
        }
        return changes;
    }

    private boolean shouldTerminate(final int changes) {
        return changes == 0 || changes < inputs.length * Math.abs(this.topK) * precision;
    }

    @Override
    public ApproxNearestNeighbors<T> me() {
        return this;
    }

    @Override
    public void release() {
        this.inputs = null;
    }

    private class InitTask implements Runnable {

        private final RleDecoder rleDecoder;

        InitTask(final Supplier<RleDecoder> rleDecoderFactory) {
            rleDecoder = rleDecoderFactory.get();
        }

        @Override
        public void run() {
            for (; ; ) {
                final long nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= inputs.length || !running()) {
                    return;
                }

                int index = Math.toIntExact(nodeId);
                AnnTopKConsumer consumer = topKConsumers[index];
                T me = inputs[index];
                Set<Integer> randomNeighbors = ANNUtils.selectRandomNeighbors(Math.abs(topK), inputs.length, index, random);

                for (Integer neighborIndex : randomNeighbors) {
                    T neighbour = inputs[neighborIndex];
                    SimilarityResult result = similarityComputer.similarity(rleDecoder, me, neighbour, similarityCutoff);
                    if(result != null) {
                        consumer.applyAsInt(result);
                    }
                }

            }
        }
    }

    interface NeighborhoodTask extends Runnable {
        int mergeInto(AnnTopKConsumer[] target);
    }

    private class SetupTask implements Runnable {
        private final NewOldGraph graph;
        private final HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer oldRelationshipBuilder;
        private final HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer newRelationshipBuilder;
        private final double sampleSize;
        private final RoaringBitmap[] visitedRelationships;
        private final long startNodeId;
        private final long nodeCount;

        SetupTask(
                final NewOldGraph graph,
                RoaringBitmap[] visitedRelationships,
                final HugeRelationshipsBuilder oldRelationshipBuilder,
                final HugeRelationshipsBuilder newRelationshipBuilder,
                final double sampleSize,
                long startNodeId,
                long nodeCount) {
            this.graph = graph;
            this.visitedRelationships = visitedRelationships;
            this.oldRelationshipBuilder = oldRelationshipBuilder.withBuffer();
            this.newRelationshipBuilder = newRelationshipBuilder.withBuffer();
            this.sampleSize = sampleSize;
            this.startNodeId = startNodeId;
            this.nodeCount = nodeCount;
        }

        @Override
        public void run() {
            long endNodeId = startNodeId + nodeCount;
            for (long longNodeId = startNodeId; longNodeId < endNodeId; longNodeId++) {
                if (!running()) {
                    return;
                }
                int nodeId = Math.toIntExact(longNodeId);

                for (LongCursor neighbor : graph.findOldNeighbors(longNodeId)) {
                    oldRelationshipBuilder.addRelationship(longNodeId, neighbor.value);
                }

                long[] potentialNewNeighbors = graph.findNewNeighbors(longNodeId).toArray();
                long[] newOutgoingNeighbors = sampling ? ANNUtils.sampleNeighbors(
                        potentialNewNeighbors,
                        sampleSize,
                        random) : potentialNewNeighbors;
                for (long neighbor : newOutgoingNeighbors) {
                    newRelationshipBuilder.addRelationship(longNodeId, neighbor);
                }

                for (Long neighbor : newOutgoingNeighbors) {
                    int neighborNodeId = Math.toIntExact(neighbor);
                    visitedRelationships[nodeId].add(neighborNodeId);
                }
            }
            oldRelationshipBuilder.flushAll();
            newRelationshipBuilder.flushAll();
        }
    }

    private class ComputeTask implements NeighborhoodTask {
        private final RleDecoder rleDecoder;
        private final AnnTopKConsumer[] localTopKConsumers;
        private final Graph oldGraph;
        private final Graph newGraph;
        private final double sampleRate;

        ComputeTask(
                final Supplier<RleDecoder> rleDecoderFactory,
                final int length,
                final HugeGraph oldGraph,
                final HugeGraph newGraph,
                final double sampleRate) {
            this.rleDecoder = rleDecoderFactory.get();
            this.localTopKConsumers = AnnTopKConsumer.initializeTopKConsumers(length, topK);
            this.oldGraph = oldGraph.concurrentCopy();
            this.newGraph = newGraph.concurrentCopy();
            this.sampleRate = sampleRate;
        }

        @Override
        public void run() {
            for (; ; ) {
                final long nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= inputs.length || !running()) {
                    return;
                }

                LongHashSet oldNeighbors = getNeighbors(nodeId, oldGraph);
                long[] newNeighbors = getNeighbors(nodeId, newGraph).toArray();

                for (int sourceIndex = 0; sourceIndex < newNeighbors.length; sourceIndex++) {
                    int sourceNodeId = Math.toIntExact(newNeighbors[sourceIndex]);
                    T sourceNode = inputs[sourceNodeId];
                    for (int targetIndex = sourceIndex + 1; targetIndex < newNeighbors.length; targetIndex++) {
                        int targetNodeId = Math.toIntExact(newNeighbors[targetIndex]);
                        T targetNode = inputs[targetNodeId];
                        SimilarityResult result = similarityComputer.similarity(
                                rleDecoder,
                                sourceNode,
                                targetNode,
                                similarityCutoff);
                        if (result != null) {
                            localTopKConsumers[sourceNodeId].applyAsInt(result);
                            localTopKConsumers[targetNodeId].applyAsInt(result.reverse());
                        }
                    }

                    for (LongCursor cursor : oldNeighbors) {
                        int targetNodeId = Math.toIntExact(cursor.value);
                        T targetNode = inputs[targetNodeId];
                        if (sourceNodeId != targetNodeId) {
                            SimilarityResult result = similarityComputer.similarity(
                                    rleDecoder,
                                    sourceNode,
                                    targetNode,
                                    similarityCutoff);
                            if (result != null) {
                                localTopKConsumers[sourceNodeId].applyAsInt(result);
                                localTopKConsumers[targetNodeId].applyAsInt(result.reverse());
                            }
                        }
                    }
                }
            }
        }

        private LongHashSet getNeighbors(final long nodeId, final Graph graph) {
            long[] potentialIncomingNeighbors = findNeighbors(nodeId, graph, Direction.INCOMING).toArray();
            long[] incomingNeighbors = sampling
                    ? ANNUtils.sampleNeighbors(potentialIncomingNeighbors, sampleRate, random)
                    : potentialIncomingNeighbors;

            LongHashSet outgoingNeighbors = findNeighbors(nodeId, graph, Direction.OUTGOING);

            LongHashSet newNeighbors = new LongHashSet();
            newNeighbors.addAll(incomingNeighbors);
            newNeighbors.addAll(outgoingNeighbors);
            return newNeighbors;
        }

        public int mergeInto(AnnTopKConsumer[] target) {
            int changes = 0;
            for (int i = 0; i < target.length; i++) {
                changes += target[i].apply(this.localTopKConsumers[i]);
            }
            return changes;
        }
    }

    public AnnTopKConsumer[] result() {
        return topKConsumers;
    }

    public int iterations() {
        return actualIterations.get();
    }

    private LongHashSet findNeighbors(
            final long nodeId,
            final RelationshipIterator graph, final Direction direction) {
        LongHashSet neighbors = new LongHashSet();

        graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId) -> {
            neighbors.add(targetNodeId);
            return true;
        });
        return neighbors;
    }
}
