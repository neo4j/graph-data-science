/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.similarity;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
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
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class ApproxNearestNeighborsAlgorithm<INPUT extends SimilarityInput> extends SimilarityAlgorithm<ApproxNearestNeighborsAlgorithm<INPUT>, INPUT> {

    private final ApproximateNearestNeighborsConfig config;
    private final SimilarityAlgorithm<?, INPUT> algorithm;
    private final Log log;
    private final ExecutorService executor;
    private final AtomicLong nodeQueue;
    private final AtomicInteger actualIterations;
    private final Random random;

    public ApproxNearestNeighborsAlgorithm(
        ApproximateNearestNeighborsConfig config,
        SimilarityAlgorithm<?, INPUT> algorithm,
        GraphDatabaseAPI api,
        Log log
    ) {
        super(config, api);
        this.config = config;
        this.algorithm = algorithm;
        this.log = log;

        this.executor = Pools.DEFAULT;
        this.nodeQueue = new AtomicLong();
        this.actualIterations = new AtomicInteger();
        this.random = new Random(config.randomSeed());
    }

    @Override
    INPUT[] prepareInputs(Object rawData, SimilarityConfig config) {
        return algorithm.prepareInputs(rawData, config);
    }

    @Override
    protected Supplier<RleDecoder> createDecoderFactory(int size) {
        return algorithm.createDecoderFactory(size);
    }

    @Override
    Supplier<RleDecoder> inputDecoderFactory(INPUT[] inputs) {
        return algorithm.inputDecoderFactory(inputs);
    }

    @Override
    SimilarityComputer<INPUT> similarityComputer(
        Double skipValue,
        int[] sourceIndexIds,
        int[] targetIndexIds
    ) {
        return algorithm.similarityComputer(skipValue, sourceIndexIds, targetIndexIds);
    }

    @Override
    protected Stream<SimilarityResult> similarityStream(
        INPUT[] inputs,
        int[] sourceIndexIds,
        int[] targetIndexIds,
        SimilarityComputer<INPUT> computer,
        Supplier<RleDecoder> decoderFactory,
        double cutoff,
        int topK
    ) {
        double sampleSize = Math.min(config.p(), 1.0) * Math.abs(config.topK());
        int inputSize = inputs.length;
        AnnTopKConsumer[] topKConsumers = AnnTopKConsumer.initializeTopKConsumers(inputSize, topK);
        Collection<Runnable> tasks = createInitTasks(inputs, topKConsumers, decoderFactory, computer);

        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);

        IdsAndProperties nodes = buildNodes(inputs);

        RoaringBitmap[] visitedRelationships = ANNUtils.initializeRoaringBitmaps(inputSize);
        RoaringBitmap[] tempVisitedRelationships = ANNUtils.initializeRoaringBitmaps(inputSize);

        for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
            for (int i = 0; i < inputSize; i++) {
                visitedRelationships[i] = RoaringBitmap.or(visitedRelationships[i], tempVisitedRelationships[i]);
            }
            tempVisitedRelationships = ANNUtils.initializeRoaringBitmaps(inputSize);

            HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer relationshipBuilder = new HugeRelationshipsBuilder(
                nodes).withBuffer();
            relationshipBuilder.addRelationshipsFrom(topKConsumers);
            Relationships hugeRels = relationshipBuilder.build();

            HugeGraph hugeGraph = ANNUtils.hugeGraph(nodes, hugeRels);
            HugeRelationshipsBuilder oldRelationshipsBuilder = new HugeRelationshipsBuilder(nodes);
            HugeRelationshipsBuilder newRelationshipBuilder = new HugeRelationshipsBuilder(nodes);

            Collection<Runnable> setupTasks = setupTasks(
                sampleSize,
                inputSize,
                visitedRelationships,
                tempVisitedRelationships,
                hugeGraph,
                oldRelationshipsBuilder,
                newRelationshipBuilder
            );
            ParallelUtil.runWithConcurrency(1, setupTasks, executor);

            HugeGraph oldHugeGraph = ANNUtils.hugeGraph(nodes, oldRelationshipsBuilder.build());
            HugeGraph newHugeGraph = ANNUtils.hugeGraph(nodes, newRelationshipBuilder.build());

            Collection<NeighborhoodTask> computeTasks = computeTasks(
                sampleSize,
                inputs,
                computer,
                newHugeGraph,
                decoderFactory,
                oldHugeGraph
            );
            ParallelUtil.runWithConcurrency(config.concurrency(), computeTasks, executor);

            int changes = mergeConsumers(computeTasks, topKConsumers);

            log.info("ANN: Changes in iteration %d: %d", iteration, changes);
            actualIterations.set(iteration);

            if (shouldTerminate(changes, inputSize, config.topK())) {
                break;
            }

        }
        return Arrays.stream(topKConsumers).flatMap(AnnTopKConsumer::stream);
    }

    private Collection<Runnable> setupTasks(
        double sampleSize,
        int inputSize,
        RoaringBitmap[] visitedRelationships,
        RoaringBitmap[] tempVisitedRelationships,
        HugeGraph hugeGraph,
        HugeRelationshipsBuilder oldRelationshipsBuilder,
        HugeRelationshipsBuilder newRelationshipBuilder
    ) {
        int batchSize = ParallelUtil.adjustedBatchSize(inputSize, config.concurrency(), 100);
        int numberOfBatches = (inputSize / batchSize) + 1;
        Collection<Runnable> setupTasks = new ArrayList<>(numberOfBatches);

        long startNodeId = 0L;
        for (int batch = 0; batch < numberOfBatches; batch++) {
            long nodeCount = Math.min(batchSize, inputSize - (batch * batchSize));
            setupTasks.add(
                new SetupTask(
                    new NewOldGraph(hugeGraph, visitedRelationships),
                    tempVisitedRelationships,
                    oldRelationshipsBuilder,
                    newRelationshipBuilder,
                    sampleSize,
                    startNodeId,
                    nodeCount
                )
            );
            startNodeId += nodeCount;
        }
        return setupTasks;
    }

    private List<Runnable> createInitTasks(
        INPUT[] inputs,
        AnnTopKConsumer[] topKConsumers,
        Supplier<RleDecoder> rleDecoderFactory,
        SimilarityComputer<INPUT> similarityComputer
    ) {
        nodeQueue.set(0);
        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < config.concurrency(); i++) {
            tasks.add(new InitTask(inputs, topKConsumers, rleDecoderFactory, similarityComputer));
        }
        return tasks;
    }

    private Collection<NeighborhoodTask> computeTasks(
        double sampleSize,
        INPUT[] inputs,
        SimilarityComputer<INPUT> similarityComputer,
        HugeGraph oldHugeGraph, Supplier<RleDecoder> rleDecoderFactory, HugeGraph newHugeGraph
    ) {
        nodeQueue.set(0);
        Collection<NeighborhoodTask> computeTasks = new ArrayList<>();
        for (int i = 0; i < config.concurrency(); i++) {
            computeTasks.add(
                new ComputeTask(
                    inputs,
                    similarityComputer,
                    rleDecoderFactory,
                    inputs.length,
                    oldHugeGraph,
                    newHugeGraph,
                    sampleSize
                )
            );
        }
        return computeTasks;
    }

    private IdsAndProperties buildNodes(INPUT[] inputs) {
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(inputs.length, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);
        long maxNodeId = 0L;

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), inputs.length, false);

        for (INPUT input : inputs) {
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

    private int mergeConsumers(
        Iterable<NeighborhoodTask> neighborhoodTasks,
        AnnTopKConsumer[] topKConsumers
    ) {
        int changes = 0;
        for (NeighborhoodTask task : neighborhoodTasks) {
            changes += task.mergeInto(topKConsumers);
        }
        return changes;
    }

    private boolean shouldTerminate(int changes, int inputSize, int topK) {
        return changes == 0 || changes < inputSize * Math.abs(topK) * config.precision();
    }

    private LongHashSet findNeighbors(
        long nodeId,
        RelationshipIterator graph, Direction direction
    ) {
        LongHashSet neighbors = new LongHashSet();
        graph.forEachRelationship(nodeId, direction, (sourceNodeId, targetNodeId) -> {
            neighbors.add(targetNodeId);
            return true;
        });
        return neighbors;
    }

    private class InitTask implements Runnable {

        private final INPUT[] inputs;
        private final AnnTopKConsumer[] topKConsumers;
        private final RleDecoder rleDecoder;
        private final SimilarityComputer<INPUT> similarityComputer;

        InitTask(
            INPUT[] inputs,
            AnnTopKConsumer[] topKConsumers,
            Supplier<RleDecoder> rleDecoderFactory,
            SimilarityComputer<INPUT> similarityComputer
        ) {
            this.inputs = inputs;
            this.topKConsumers = topKConsumers;
            rleDecoder = rleDecoderFactory.get();
            this.similarityComputer = similarityComputer;
        }

        @Override
        public void run() {
            for (; ; ) {
                long nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= inputs.length || !running()) {
                    return;
                }

                int index = Math.toIntExact(nodeId);
                AnnTopKConsumer consumer = topKConsumers[index];
                INPUT me = inputs[index];
                Set<Integer> randomNeighbors = ANNUtils.selectRandomNeighbors(
                    Math.abs(config.topK()),
                    inputs.length,
                    index,
                    random
                );

                for (Integer neighborIndex : randomNeighbors) {
                    INPUT neighbour = inputs[neighborIndex];
                    SimilarityResult result = similarityComputer.similarity(
                        rleDecoder,
                        me,
                        neighbour,
                        config.similarityCutoff()
                    );
                    if (result != null) {
                        consumer.applyAsInt(result);
                    }
                }

            }
        }
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
            NewOldGraph graph,
            RoaringBitmap[] visitedRelationships,
            HugeRelationshipsBuilder oldRelationshipBuilder,
            HugeRelationshipsBuilder newRelationshipBuilder,
            double sampleSize,
            long startNodeId,
            long nodeCount
        ) {
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
                long[] newOutgoingNeighbors = config.sampling() ? ANNUtils.sampleNeighbors(
                    potentialNewNeighbors,
                    sampleSize,
                    random
                ) : potentialNewNeighbors;
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

    interface NeighborhoodTask extends Runnable {
        int mergeInto(AnnTopKConsumer[] target);
    }

    private class ComputeTask implements NeighborhoodTask {
        private final INPUT[] inputs;
        private final SimilarityComputer<INPUT> similarityComputer;
        private final RleDecoder rleDecoder;
        private final AnnTopKConsumer[] localTopKConsumers;
        private final Graph oldGraph;
        private final Graph newGraph;
        private final double sampleRate;

        ComputeTask(
            INPUT[] inputs,
            SimilarityComputer<INPUT> similarityComputer,
            Supplier<RleDecoder> rleDecoderFactory,
            int length,
            HugeGraph oldGraph,
            HugeGraph newGraph,
            double sampleRate
        ) {
            this.inputs = inputs;
            this.similarityComputer = similarityComputer;
            this.rleDecoder = rleDecoderFactory.get();
            this.localTopKConsumers = AnnTopKConsumer.initializeTopKConsumers(length, config.topK());
            this.oldGraph = oldGraph.concurrentCopy();
            this.newGraph = newGraph.concurrentCopy();
            this.sampleRate = sampleRate;
        }

        @Override
        public void run() {
            for (; ; ) {
                long nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= inputs.length || !running()) {
                    return;
                }

                LongHashSet oldNeighbors = getNeighbors(nodeId, oldGraph);
                long[] newNeighbors = getNeighbors(nodeId, newGraph).toArray();

                for (int sourceIndex = 0; sourceIndex < newNeighbors.length; sourceIndex++) {
                    int sourceNodeId = Math.toIntExact(newNeighbors[sourceIndex]);
                    INPUT sourceNode = inputs[sourceNodeId];
                    for (int targetIndex = sourceIndex + 1; targetIndex < newNeighbors.length; targetIndex++) {
                        int targetNodeId = Math.toIntExact(newNeighbors[targetIndex]);
                        INPUT targetNode = inputs[targetNodeId];
                        SimilarityResult result = similarityComputer.similarity(
                            rleDecoder,
                            sourceNode,
                            targetNode,
                            config.similarityCutoff()
                        );
                        if (result != null) {
                            localTopKConsumers[sourceNodeId].applyAsInt(result);
                            localTopKConsumers[targetNodeId].applyAsInt(result.reverse());
                        }
                    }

                    for (LongCursor cursor : oldNeighbors) {
                        int targetNodeId = Math.toIntExact(cursor.value);
                        INPUT targetNode = inputs[targetNodeId];
                        if (sourceNodeId != targetNodeId) {
                            SimilarityResult result = similarityComputer.similarity(
                                rleDecoder,
                                sourceNode,
                                targetNode,
                                config.similarityCutoff()
                            );
                            if (result != null) {
                                localTopKConsumers[sourceNodeId].applyAsInt(result);
                                localTopKConsumers[targetNodeId].applyAsInt(result.reverse());
                            }
                        }
                    }
                }
            }
        }

        private LongHashSet getNeighbors(long nodeId, Graph graph) {
            long[] potentialIncomingNeighbors = findNeighbors(nodeId, graph, Direction.INCOMING).toArray();
            long[] incomingNeighbors = config.sampling()
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

}
