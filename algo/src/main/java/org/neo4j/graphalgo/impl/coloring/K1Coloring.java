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
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

public class K1Coloring extends Algorithm<K1Coloring> {

    private final AllocationTracker tracker;
    private final long batchSize;
    private final ExecutorService executor;
    private final int threadSize;

    private BitSet nodesToColor;

    private Graph graph;
    private HugeLongArray colors;
    private long ranIterations;

    public K1Coloring(
            Graph graph,
            int minBatchSize,
            int concurrency,
            ExecutorService executor,
            AllocationTracker tracker
    ) {
        this.graph = graph;
        this.executor = executor;
        this.tracker = tracker;

        this.nodesToColor = new BitSet(graph.nodeCount());
        this.batchSize = ParallelUtil.adjustedBatchSize(
            graph.nodeCount(),
            concurrency,
            minBatchSize,
            Integer.MAX_VALUE);
        long threadSize = ParallelUtil.threadCount(minBatchSize, graph.nodeCount());
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                "Too many nodes (%d) to run union find with the given concurrency (%d) and batchSize (%d)",
                graph.nodeCount(),
                concurrency,
                minBatchSize));
        }
        this.threadSize = (int) threadSize;
    }

    @Override
    public K1Coloring me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        nodesToColor = null;
    }

    public long ranIterations() {
        return ranIterations;
    }


    public HugeLongArray colors() {
        return colors;
    }

    public K1Coloring compute(Direction direction, long maxIterations) {
        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }

        colors = HugeLongArray.newArray(graph.nodeCount(), tracker);
        colors.setAll((nodeId) -> nodeId);

        ranIterations = 0L;
        nodesToColor.set(0, graph.nodeCount());

        while (ranIterations < maxIterations && !nodesToColor.isEmpty()) {
            runColoring();
            runValidation();
            ++ranIterations;
        }

        return me();
    }

    private void runColoring() {
        Collection<ColoringStep> steps = new ArrayList<>(threadSize);
        for (long i = 0L; i < graph.nodeCount(); i += batchSize) {
            ColoringStep step = new ColoringStep(
                graph,
                Direction.BOTH,
                colors,
                nodesToColor,
                i,
                batchSize
            );

            steps.add(step);
        }

        ParallelUtil.run(steps, executor);
    }

    private void runValidation() {
        Collection<ValidationStep> steps = new ArrayList<>(threadSize);
        BitSet nextNodesToColor = new BitSet(graph.nodeCount());
        for (long i = 0L; i < graph.nodeCount(); i += batchSize) {
            ValidationStep step = new ValidationStep(
                graph,
                Direction.BOTH,
                colors,
                nodesToColor,
                nextNodesToColor,
                i,
                batchSize
            );

            steps.add(step);
        }

        ParallelUtil.run(steps, executor);

        this.nodesToColor = nextNodesToColor;
    }
}
