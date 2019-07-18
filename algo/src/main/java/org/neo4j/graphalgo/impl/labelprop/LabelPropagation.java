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

import com.carrotsearch.hppc.LongDoubleScatterMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.huge.loader.HugeNullWeightMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LabelPropagation extends Algorithm<LabelPropagation> {

    private static final long[] EMPTY_LONGS = new long[0];

    public static final String SEED_TYPE = "seed";
    public static final String WEIGHT_TYPE = "weight";

    public static final PropertyTranslator.OfLong<Labels> LABEL_TRANSLATOR = Labels::labelFor;

    private final long nodeCount;
    private final AllocationTracker tracker;
    private final HugeWeightMapping nodeProperties;
    private final HugeWeightMapping nodeWeights;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;

    private Graph graph;
    private Labels labels;
    private final long maxLabelId;
    private long ranIterations;
    private boolean didConverge;

    public LabelPropagation(
            Graph graph,
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

        HugeWeightMapping seedProperty = graph.nodeProperties(SEED_TYPE);
        if (seedProperty == null) {
            seedProperty = new HugeNullWeightMap(0.0);
        }
        this.nodeProperties = seedProperty;

        HugeWeightMapping weightProperty = graph.nodeProperties(WEIGHT_TYPE);
        if (weightProperty == null) {
            weightProperty = new HugeNullWeightMap(1.0);
        }
        this.nodeWeights = weightProperty;
        maxLabelId = nodeProperties.getMaxValue();
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
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        if (labels == null || labels.size() != nodeCount) {
            labels = initialLabels(nodeCount, tracker);
        }

        ranIterations = 0L;
        didConverge = false;

        List<BaseStep> baseSteps = baseSteps(direction);

        long currentIteration = 0L;
        while (running() && currentIteration < maxIterations) {
            ParallelUtil.runWithConcurrency(concurrency, baseSteps, 1L, MICROSECONDS, terminationFlag, executor);
            ++currentIteration;
        }

        long maxIteration = 0L;
        boolean converged = true;
        for (BaseStep baseStep : baseSteps) {
            Step current = baseStep.current;
            if (current instanceof ComputeStep) {
                ComputeStep step = (ComputeStep) current;
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

    private Labels initialLabels(final long nodeCount, final AllocationTracker tracker) {
        return new Labels(HugeLongArray.newArray(nodeCount, tracker));
    }

    private List<BaseStep> baseSteps(Direction direction) {
        long nodeCount = graph.nodeCount();
        long batchSize = ParallelUtil.adjustBatchSize(nodeCount, (long) this.batchSize);

        Collection<PrimitiveLongIterable> nodeBatches = LazyBatchCollection.of(
                nodeCount,
                batchSize,
                (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L));

        int threads = nodeBatches.size();
        List<BaseStep> tasks = new ArrayList<>(threads);
        for (PrimitiveLongIterable iter : nodeBatches) {
            InitStep initStep = new InitStep(
                    graph,
                    nodeProperties,
                    nodeWeights,
                    iter,
                    labels,
                    getProgressLogger(),
                    direction,
                    maxLabelId
            );
            BaseStep task = new BaseStep(initStep);
            tasks.add(task);
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, 1, MILLISECONDS, terminationFlag, executor);
        return tasks;
    }

    // Labels

    public static final class Labels {
        final HugeLongArray labels;

        Labels(final HugeLongArray labels) {
            this.labels = labels;
        }

        public long labelFor(final long nodeId) {
            return labels.get(nodeId);
        }

        public void setLabelFor(final long nodeId, final long label) {
            labels.set(nodeId, label);
        }

        public long size() {
            return labels.size();
        }

        @Override
        public String toString() {
            return "Labels{" +
                   "labels=" + labels +
                   '}';
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

    private static final class InitStep implements Step {

        private final HugeWeightMapping nodeProperties;
        private final Labels existingLabels;
        private final PrimitiveLongIterable nodes;
        private final Graph graph;
        private final HugeWeightMapping nodeWeights;
        private final ProgressLogger progressLogger;
        private final Direction direction;
        private final long maxLabelId;

        private InitStep(
                Graph graph,
                HugeWeightMapping nodeProperties,
                HugeWeightMapping nodeWeights,
                PrimitiveLongIterable nodes,
                Labels existingLabels,
                ProgressLogger progressLogger,
                Direction direction,
                long maxLabelId) {
            this.nodeProperties = nodeProperties;
            this.existingLabels = existingLabels;
            this.nodes = nodes;
            this.graph = graph;
            this.nodeWeights = nodeWeights;
            this.progressLogger = progressLogger;
            this.direction = direction;
            this.maxLabelId = maxLabelId;
        }

        @Override
        public final void run() {
            PrimitiveLongIterator iterator = nodes.iterator();
            while (iterator.hasNext()) {
                long nodeId = iterator.next();
                double existingLabelValue = nodeProperties.nodeWeight(nodeId, Double.NaN);
                long existingLabel = Double.isNaN(existingLabelValue)
                        ? maxLabelId + graph.toOriginalNodeId(nodeId) + 1L
                        : (long) existingLabelValue;
                existingLabels.setLabelFor(nodeId, existingLabel);
            }
        }

        @Override
        public final Step next() {
            return new ComputeStep(
                    graph,
                    nodeWeights,
                    progressLogger,
                    direction,
                    existingLabels,
                    nodes
            );
        }
    }

    private static final class ComputeStep implements Step, WeightedRelationshipConsumer {

        private final RelationshipIterator localRelationshipIterator;
        private final Direction direction;
        private final HugeWeightMapping nodeWeights;
        private final Labels existingLabels;
        private final LongDoubleScatterMap votes;
        private final PrimitiveLongIterable nodes;
        private final ProgressLogger progressLogger;
        private final double maxNode;

        private ComputeStep(
                Graph graph,
                HugeWeightMapping nodeWeights,
                ProgressLogger progressLogger,
                Direction direction,
                Labels existingLabels,
                PrimitiveLongIterable nodes) {
            this.existingLabels = existingLabels;
            this.progressLogger = progressLogger;
            this.maxNode = (double) graph.nodeCount() - 1L;
            this.localRelationshipIterator = graph.concurrentCopy();
            this.nodeWeights = nodeWeights;
            this.direction = direction;
            this.nodes = nodes;
            this.votes = new LongDoubleScatterMap();
        }

        private boolean didChange = true;
        long iteration = 0L;

        @Override
        public final void run() {
            if (this.didChange) {
                iteration++;
                this.didChange = computeAll();
                if (!this.didChange) {
                    release();
                }
            }
        }

        boolean computeAll() {
            return iterateAll(nodes.iterator());
        }

        void forEach(final long nodeId) {
            localRelationshipIterator.forEachRelationship(nodeId, direction, this);
        }

        double weightOf(final long candidate, final double relationshipWeight) {
            double nodeWeight = nodeWeights.nodeWeight(candidate);
            return relationshipWeight * nodeWeight;
        }

        @Override
        public boolean accept(final long sourceNodeId, final long targetNodeId, final double weight) {
            castVote(targetNodeId, weight);
            return true;
        }

        @Override
        public final Step next() {
            return this;
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

        final void castVote(long candidate, double weight) {
            weight = weightOf(candidate, weight);
            long label = existingLabels.labelFor(candidate);
            votes.addTo(label, weight);
        }

        final boolean compute(long nodeId, boolean didChange) {
            votes.clear();
            long label = existingLabels.labelFor(nodeId);
            long previous = label;
            forEach(nodeId);
            double weight = Double.NEGATIVE_INFINITY;
            for (LongDoubleCursor vote : votes) {
                if (weight < vote.value) {
                    weight = vote.value;
                    label = vote.key;
                } else if (weight == vote.value) {
                    if (label > vote.key) {
                        label = vote.key;
                    }
                }
            }
            if (label != previous) {
                existingLabels.setLabelFor(nodeId, label);
                return true;
            }
            return didChange;
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
}
