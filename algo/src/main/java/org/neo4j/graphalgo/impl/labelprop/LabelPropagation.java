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
package org.neo4j.graphalgo.impl.labelprop;

import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.RandomLongIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.carrotsearch.hppc.Containers.DEFAULT_EXPECTED_ELEMENTS;
import static com.carrotsearch.hppc.HashContainers.DEFAULT_LOAD_FACTOR;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LabelPropagation extends Algorithm<LabelPropagation> {

    private static final long[] EMPTY_LONGS = new long[0];

    public static final String PARTITION_TYPE = "property";
    public static final String WEIGHT_TYPE = "weight";

    public static final PropertyTranslator.OfLong<Labels> LABEL_TRANSLATOR =
            Labels::labelFor;

    private final long nodeCount;
    private final AllocationTracker tracker;
    private final HugeWeightMapping nodeProperties;
    private final HugeWeightMapping nodeWeights;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;

    private final ThreadLocal<RelationshipIterator> localGraphs;

    private Graph graph;
    private Labels labels;
    private long ranIterations;
    private boolean didConverge;

    public LabelPropagation(
            Graph graph,
            NodeProperties nodeProperties,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties.nodeProperties(PARTITION_TYPE);
        this.nodeWeights = nodeProperties.nodeProperties(WEIGHT_TYPE);
        localGraphs = ThreadLocal.withInitial(graph::concurrentCopy);
    }

    @Override
    public LabelPropagation me() {
        return this;
    }

    @Override
    public LabelPropagation release() {
        graph = null;
        return me();
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public Labels labels() {
        return labels;
    }

    public LabelPropagation compute(Direction direction, long maxIterations) {
        return compute(direction, maxIterations, DefaultRandom.INSTANCE);
    }

    public LabelPropagation compute(Direction direction, long maxIterations, long randomSeed) {
        return compute(direction, maxIterations, new Random(randomSeed));
    }

    public LabelPropagation compute(Direction direction, long maxIterations, Random random) {
        return compute(direction, maxIterations, new ProvidedRandom(random));
    }

    private HugeLabelArray initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new HugeLabelArray(HugeLongArray.newArray(nodeCount, tracker));
    }

    private Initialization initStep(
            final Graph graph,
            final Labels labels,
            final HugeWeightMapping nodeProperties,
            final HugeWeightMapping nodeWeights,
            final Direction direction,
            final ProgressLogger progressLogger,
            final RandomProvider randomProvider,
            final RandomLongIterable nodes) {
        return new InitStep(
                nodeProperties,
                labels,
                nodes,
                localGraphs,
                nodeWeights,
                progressLogger,
                direction,
                graph.nodeCount() - 1L,
                randomProvider
        );
    }

    private long adjustBatchSize(long nodeCount, long batchSize) {
        if (batchSize <= 0L) {
            batchSize = 1L;
        }
        batchSize = BitUtil.nextHighestPowerOfTwo(batchSize);
        while (((nodeCount + batchSize + 1L) / batchSize) > (long) Integer.MAX_VALUE) {
            batchSize = batchSize << 1;
        }
        return batchSize;
    }

    private List<BaseStep> baseSteps(Direction direction, RandomProvider random) {

        long nodeCount = graph.nodeCount();
        long batchSize = adjustBatchSize(nodeCount, (long) this.batchSize);

        Collection<RandomLongIterable> nodeBatches = LazyBatchCollection.of(
                nodeCount,
                batchSize,
                (start, length) -> new RandomLongIterable(start, start + length, random.randomForNewIteration()));

        int threads = nodeBatches.size();
        List<BaseStep> tasks = new ArrayList<>(threads);
        for (RandomLongIterable iter : nodeBatches) {
            Initialization initStep = initStep(
                    graph,
                    labels,
                    nodeProperties,
                    nodeWeights,
                    direction,
                    getProgressLogger(),
                    random,
                    iter
            );
            BaseStep task = new BaseStep(initStep);
            tasks.add(task);
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, 1, MILLISECONDS, terminationFlag, executor);
        return tasks;
    }

    private LabelPropagation compute(
            Direction direction,
            long maxIterations,
            RandomProvider random) {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        if (labels == null || labels.size() != nodeCount) {
            labels = initialLabels(nodeCount, tracker);
        }

        ranIterations = 0L;
        didConverge = false;

        List<BaseStep> baseSteps = baseSteps(direction, random);

        long currentIteration = 0L;
        while (running() && currentIteration < maxIterations) {
            ParallelUtil.runWithConcurrency(concurrency, baseSteps, 1L, MICROSECONDS, terminationFlag, executor);
            ++currentIteration;
        }

        long maxIteration = 0L;
        boolean converged = true;
        for (BaseStep baseStep : baseSteps) {
            Step current = baseStep.current;
            if (current instanceof Computation) {
                Computation step = (Computation) current;
                if (step.iteration > maxIteration) {
                    maxIteration = step.iteration;
                }
                converged = converged && !step.didChange;
                step.release();
            }
        }

        ranIterations = maxIteration;
        didConverge = converged;

        return me();
    }

    // Labels

    public interface Labels {
        long labelFor(long nodeId);

        void setLabelFor(long nodeId, long label);

        long size();
    }

    static final class HugeLabelArray implements Labels {
        final HugeLongArray labels;

        HugeLabelArray(final HugeLongArray labels) {
            this.labels = labels;
        }

        @Override
        public long labelFor(final long nodeId) {
            return labels.get(nodeId);
        }

        @Override
        public void setLabelFor(final long nodeId, final long label) {
            labels.set(nodeId, label);
        }

        @Override
        public long size() {
            return labels.size();
        }
    }

    // Steps

    interface Step extends Runnable {
        @Override
        void run();

        Step next();
    }

    static final class BaseStep implements Runnable {

        private Step current;

        BaseStep(final Step current) {
            this.current = current;
        }

        @Override
        public void run() {
            current.run();
            current = current.next();
        }
    }

    static abstract class Initialization implements Step {
        abstract void setExistingLabels();

        abstract Computation computeStep();

        @Override
        public final void run() {
            setExistingLabels();
        }

        @Override
        public final Step next() {
            return computeStep();
        }
    }

    private static final class InitStep extends Initialization {

        private final HugeWeightMapping nodeProperties;
        private final Labels existingLabels;
        private final RandomLongIterable nodes;
        private final ThreadLocal<RelationshipIterator> graph;
        private final HugeWeightMapping nodeWeights;
        private final ProgressLogger progressLogger;
        private final Direction direction;
        private final long maxNode;
        private final RandomProvider random;

        private InitStep(
                HugeWeightMapping nodeProperties,
                Labels existingLabels,
                RandomLongIterable nodes,
                ThreadLocal<RelationshipIterator> graph,
                HugeWeightMapping nodeWeights,
                ProgressLogger progressLogger,
                Direction direction,
                long maxNode,
                RandomProvider random) {
            this.nodeProperties = nodeProperties;
            this.existingLabels = existingLabels;
            this.nodes = nodes;
            this.graph = graph;
            this.nodeWeights = nodeWeights;
            this.progressLogger = progressLogger;
            this.direction = direction;
            this.maxNode = maxNode;
            this.random = random;
        }

        @Override
        void setExistingLabels() {
            PrimitiveLongIterator iterator = nodes.iterator(random.randomForNewIteration());
            while (iterator.hasNext()) {
                long nodeId = iterator.next();
                long existingLabel = (long) nodeProperties.nodeWeight(nodeId, (double) nodeId);
                existingLabels.setLabelFor(nodeId, existingLabel);
            }
        }

        @Override
        Computation computeStep() {
            return new ComputeStep(
                    graph,
                    nodeWeights,
                    progressLogger,
                    direction,
                    maxNode,
                    existingLabels,
                    nodes,
                    random
            );
        }
    }

    private static final class ComputeStep extends Computation implements WeightedRelationshipConsumer {

        private final ThreadLocal<RelationshipIterator> graphs;
        private final HugeWeightMapping nodeWeights;
        private final Direction direction;
        private final RandomLongIterable nodes;
        private RelationshipIterator graph;

        private ComputeStep(
                ThreadLocal<RelationshipIterator> graphs,
                HugeWeightMapping nodeWeights,
                ProgressLogger progressLogger,
                Direction direction,
                final long maxNode,
                Labels existingLabels,
                RandomLongIterable nodes,
                RandomProvider random) {
            super(existingLabels, progressLogger, maxNode, random);
            this.graphs = graphs;
            this.nodeWeights = nodeWeights;
            this.direction = direction;
            this.nodes = nodes;
        }

        @Override
        boolean computeAll() {
            graph = graphs.get();
            return iterateAll(nodes.iterator(randomProvider.randomForNewIteration()));
        }

        @Override
        void forEach(final long nodeId) {
            graph.forEachRelationship(nodeId, direction, this);
        }

        @Override
        double weightOf(final long nodeId, final long candidate, final double relationshipWeight) {
            double nodeWeight = nodeWeights.nodeWeight(candidate);
            return relationshipWeight * nodeWeight;
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId, final double weight) {
            castVote(sourceNodeId, targetNodeId, weight);
            return true;
        }
    }

    static abstract class Computation implements Step {

        final RandomProvider randomProvider;
        private final Labels existingLabels;
        private final ProgressLogger progressLogger;
        private final double maxNode;
        private final LongDoubleHashMap votes;

        private boolean didChange = true;
        long iteration = 0L;

        Computation(
                final Labels existingLabels,
                final ProgressLogger progressLogger,
                final long maxNode,
                final RandomProvider randomProvider) {
            this.randomProvider = randomProvider;
            this.existingLabels = existingLabels;
            this.progressLogger = progressLogger;
            this.maxNode = (double) maxNode;
            if (randomProvider.isRandom()) {
                Random random = randomProvider.randomForNewIteration();
                this.votes = new LongDoubleHashMap(
                        DEFAULT_EXPECTED_ELEMENTS,
                        (double) DEFAULT_LOAD_FACTOR,
                        HashOrderMixing.constant(random.nextLong()));
            } else {
                this.votes = new LongDoubleScatterMap();
            }
        }

        abstract boolean computeAll();

        abstract void forEach(long nodeId);

        abstract double weightOf(long nodeId, long candidate, double relationshipWeight);

        @Override
        public final void run() {
            if (this.didChange) {
                iteration++;
                didChange = computeAll();
                if (!didChange) {
                    release();
                }
            }
        }

        final boolean iterateAll(PrimitiveIntIterator nodeIds) {
            boolean didChange = false;
            while (nodeIds.hasNext()) {
                long nodeId = (long) nodeIds.next();
                didChange = compute(nodeId, didChange);
                progressLogger.logProgress((double) nodeId, maxNode);
            }
            return didChange;
        }

        final boolean iterateAll(PrimitiveLongIterator nodeIds) {
            boolean didChange = false;
            while (nodeIds.hasNext()) {
                long nodeId = nodeIds.next();
                didChange = compute(nodeId, didChange);
                progressLogger.logProgress((double) nodeId, maxNode);
            }
            return didChange;
        }

        final boolean compute(long nodeId, boolean didChange) {
            votes.clear();
            long partition = existingLabels.labelFor(nodeId);
            long previous = partition;
            forEach(nodeId);
            double weight = Double.NEGATIVE_INFINITY;
            for (LongDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    partition = vote.key;
                }
            }
            if (partition != previous) {
                existingLabels.setLabelFor(nodeId, partition);
                return true;
            }
            return didChange;
        }

        final void castVote(long nodeId, long candidate, double weight) {
            weight = weightOf(nodeId, candidate, weight);
            long partition = existingLabels.labelFor(candidate);
            votes.addTo(partition, weight);
        }

        @Override
        public final Step next() {
            return this;
        }

        final void release() {
            // the HPPC release() method allocates new arrays
            // the clear() method overwrite the existing keys with the default value
            // we want to throw away all data to allow for GC collection instead.

            if (votes.keys != null) {
                votes.keys = EMPTY_LONGS;
                votes.clear();
                votes.keys = null;
                votes.values = null;
            }
        }
    }

    // Randoms

    interface RandomProvider {
        Random randomForNewIteration();

        boolean isRandom();
    }

    private static final class ProvidedRandom implements RandomProvider {
        private final Random random;

        private ProvidedRandom(final Random random) {
            this.random = random;
        }

        @Override
        public Random randomForNewIteration() {
            return random;
        }

        @Override
        public boolean isRandom() {
            return true;
        }
    }

    private enum DefaultRandom implements RandomProvider {
        INSTANCE {
            @Override
            public Random randomForNewIteration() {
                return ThreadLocalRandom.current();
            }

            @Override
            public boolean isRandom() {
                return true;
            }
        }
    }
}
