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
package org.neo4j.graphalgo.impl.coloring;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 *<p>
 * This is a parallel implementation of the K1-Coloring algorithm.
 * The Algorithm will assign a color to every node in the graph, trying to optimize for two objectives:
 * <ul>
 *   <li> given a single node, make sure that every neigbor of that node has a different color </li>
 *   <li> use as little colors as possible </li>
 * </ul>
 * </p>
 *
 * <p>
 * The implementation is a greedy implementation based on:<br>
 * <cite>
 * Çatalyürek, Ümit V., et al.
 * "Graph coloring algorithms for multi-core and massively multithreaded architectures."
 * Parallel Computing 38.10-11 (2012): 576-594.
 * https://arxiv.org/pdf/1205.3809.pdf
 * </cite>
 * </p>
 *
 * <p>
 * The implementation is greedy, so it is not garantied to find an optimal solution, i.e. the coloring can be imperfect
 * and contain more colors as needed.
 * </p>
 */
public class K1Coloring extends Algorithm<K1Coloring> {

    private final Graph graph;
    private final long nodeCount;
    private final ExecutorService executor;
    private final AllocationTracker tracker;
    private final int minBatchSize;
    private final int concurrency;

    private final Direction direction;
    private final long maxIterations;

    private BitSet nodesToColor;
    private HugeLongArray colors;
    private long ranIterations;
    private boolean didConverge;

    private HugeLongLongMap colorMap;

    public K1Coloring(
        Graph graph,
        Direction direction,
        long maxIterations,
        int minBatchSize,
        int concurrency,
        ExecutorService executor,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.minBatchSize = minBatchSize;
        this.concurrency = concurrency;
        this.executor = executor;
        this.tracker = tracker;

        this.nodeCount = graph.nodeCount();
        this.direction = direction;
        this.maxIterations = maxIterations;

        this.nodesToColor = new BitSet(nodeCount);
    }

    @Override
    public K1Coloring me() {
        return this;
    }

    @Override
    public void release() {
        graph.release();
        nodesToColor = null;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public HugeLongLongMap colorMap() {
        if (colorMap == null) {
            this.colorMap = new HugeLongLongMap(100, tracker);
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                colorMap.addTo(colors.get(nodeId), 1L);
            }
        }
        return colorMap;
    }

    public HugeLongArray colors() {
        return colors;
    }

    public K1Coloring compute() {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        colors = HugeLongArray.newArray(nodeCount, tracker);
        colors.setAll((nodeId) -> nodeId);

        ranIterations = 0L;
        nodesToColor.set(0, nodeCount);

        while (ranIterations < maxIterations && !nodesToColor.isEmpty()) {
            runColoring(direction);
            runValidation(direction);
            ++ranIterations;
        }

        this.didConverge = ranIterations < maxIterations;
        return me();
    }

    private void runColoring(Direction direction) {
        DegreeTaskProducer<ColoringStep> producer = (batchStart, batchSize) -> new ColoringStep(
            graph.concurrentCopy(),
            direction,
            colors,
            nodesToColor,
            nodeCount,
            batchStart,
            batchSize
        );

        Collection<ColoringStep> steps = degreePartition(producer, direction);

        ParallelUtil.runWithConcurrency(concurrency, steps, executor);
    }

    private void runValidation(Direction direction) {
        BitSet nextNodesToColor = new BitSet(nodeCount);

        DegreeTaskProducer<ValidationStep> producer = (batchStart, batchSize) -> new ValidationStep(
            graph.concurrentCopy(),
            direction,
            colors,
            nodesToColor,
            nextNodesToColor,
            nodeCount,
            batchStart,
            batchSize
        );

        Collection<ValidationStep> steps = degreePartition(producer, direction);

        ParallelUtil.runWithConcurrency(concurrency, steps, executor);
        this.nodesToColor = nextNodesToColor;
    }


    private <T extends Runnable> Collection<T> degreePartition(
        DegreeTaskProducer<T> taskSupplier,
        Direction direction
    ) {

        long cumulativeDegree = direction == Direction.BOTH ? graph.relationshipCount() / 2 : graph.relationshipCount();
        long adjustedDegree = cumulativeDegree * (nodesToColor.cardinality() / nodeCount);

        long batchDegree = ParallelUtil.adjustedBatchSize(
            adjustedDegree,
            concurrency,
            minBatchSize,
            Integer.MAX_VALUE
        );

        Collection<T> tasks = new ArrayList<>(concurrency);
        long currentNode = nodesToColor.nextSetBit(0);
        long batchStart = currentNode;
        long currentDegree = 0;
        do {
            currentDegree += graph.degree(currentNode, direction);

            if (currentDegree >= batchDegree) {
                tasks.add(taskSupplier.produce(batchStart, currentDegree));
                currentNode++;
                batchStart = currentNode;
                currentDegree = 0;
            }

            currentNode = nodesToColor.nextSetBit(currentNode + 1);
        } while (currentNode >= 0 && currentNode < nodeCount);

        if (currentDegree > 0) {
            tasks.add(taskSupplier.produce(batchStart, currentDegree));
        }

        return tasks;
    }

    interface DegreeTaskProducer<T extends Runnable> {
        T produce(long batchStart, long batchSize);
    }
}
