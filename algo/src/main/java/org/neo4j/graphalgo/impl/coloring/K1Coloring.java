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
import java.util.function.Function;

public class K1Coloring extends Algorithm<K1Coloring> {

    private final Graph graph;
    private final long nodeCount;
    private final long batchSize;
    private final int threadSize;
    private final ExecutorService executor;
    private final AllocationTracker tracker;
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
        int minBatchSize,
        int concurrency,
        ExecutorService executor,
        AllocationTracker tracker,
        Direction direction, long maxIterations
    ) {
        this.graph = graph;
        this.executor = executor;
        this.tracker = tracker;

        this.nodeCount = graph.nodeCount();
        this.direction = direction;
        this.maxIterations = maxIterations;

        this.batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            minBatchSize,
            Integer.MAX_VALUE
        );
        long threadSize = ParallelUtil.threadCount(minBatchSize, nodeCount);
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                "Too many nodes (%d) to run k1 coloring with the given concurrency (%d) and batchSize (%d)",
                nodeCount,
                concurrency,
                minBatchSize
            ));
        }
        this.threadSize = (int) threadSize;
        this.concurrency = concurrency;

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

    public HugeLongLongMap getColorMap() {
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
        Function<Long, ColoringStep> producer = (batchStart) -> new ColoringStep(
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

        Function<Long, ValidationStep> producer = (batchStart) -> new ValidationStep(
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


    private <T extends Runnable> Collection<T> degreePartition(Function<Long, T> taskSupplier, Direction direction) {
        Collection<T> tasks = new ArrayList<>(threadSize);

        long cumulativeDegree = direction == Direction.BOTH ? graph.relationshipCount() / 2 : graph.relationshipCount();
        long batchDegree = cumulativeDegree * (nodesToColor.cardinality() / nodeCount) / threadSize;

        long currentNode = nodesToColor.nextSetBit(0);
        long batchStart = currentNode;
        long currentDegree = 0;
        do  {
            currentDegree += graph.degree(currentNode,direction);

            if (currentDegree >= batchDegree) {
                tasks.add(taskSupplier.apply(batchStart));
                currentNode++;
                batchStart = currentNode;
                currentDegree = 0;

            }

            currentNode = nodesToColor.nextSetBit(currentNode + 1);
        } while(currentNode >= 0 && currentNode < nodeCount);

        if(currentDegree > 0) {
            tasks.add(taskSupplier.apply(batchStart));
        }

        return tasks;
    };

}
