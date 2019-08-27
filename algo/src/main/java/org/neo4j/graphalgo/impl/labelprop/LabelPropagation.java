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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.huge.loader.HugeNullWeightMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class LabelPropagation extends Algorithm<LabelPropagation> {

    public static final String SEED_TYPE = "seed";
    public static final String WEIGHT_TYPE = "weight";

    private final long nodeCount;
    private final AllocationTracker tracker;
    private final HugeWeightMapping nodeProperties;
    private final HugeWeightMapping nodeWeights;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;

    private Graph graph;
    private HugeLongArray labels;
    private final long maxLabelId;
    private long ranIterations;
    private boolean didConverge;

    public LabelPropagation(
            Graph graph,
            int batchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker
    ) {
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
    public void release() {
        graph = null;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public HugeLongArray labels() {
        return labels;
    }

    public LabelPropagation compute(Direction direction, long maxIterations) {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        if (labels == null || labels.size() != nodeCount) {
            labels = HugeLongArray.newArray(nodeCount, tracker);
        }

        ranIterations = 0L;
        didConverge = false;

        List<StepRunner> stepRunners = stepRunners(direction);

        long currentIteration = 0L;
        while (currentIteration < maxIterations) {
            ParallelUtil.runWithConcurrency(concurrency, stepRunners, 1L, MICROSECONDS, terminationFlag, executor);
            ++currentIteration;
        }

        long maxIteration = 0L;
        boolean converged = true;
        for (StepRunner stepRunner : stepRunners) {
            Step current = stepRunner.current;
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

    private List<StepRunner> stepRunners(Direction direction) {
        long nodeCount = graph.nodeCount();
        long batchSize = ParallelUtil.adjustBatchSize(nodeCount, (long) this.batchSize);

        Collection<PrimitiveLongIterable> nodeBatches = LazyBatchCollection.of(
                nodeCount,
                batchSize,
                (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L));

        int threads = nodeBatches.size();
        List<StepRunner> tasks = new ArrayList<>(threads);
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
            StepRunner task = new StepRunner(initStep);
            tasks.add(task);
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, 1, MICROSECONDS, terminationFlag, executor);
        return tasks;
    }
}
