/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.MultiPartiteRelationshipIterator;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class WeightedDegreeCentrality extends Algorithm<WeightedDegreeCentrality, WeightedDegreeCentrality> {
    private final long nodeCount;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    private final HugeDoubleArray degrees;

    public WeightedDegreeCentrality(
        Graph graph,
        int concurrency,
        ExecutorService executor,
        AllocationTracker tracker
    ) {
        if (!graph.hasRelationshipProperty()) {
            throw new UnsupportedOperationException("WeightedDegreeCentrality requires a weight property to be loaded.");
        }

        if (concurrency <= 0) {
            concurrency = ConcurrencyConfig.DEFAULT_CONCURRENCY;
        }

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        this.nodeCount = graph.nodeCount();
        this.degrees = HugeDoubleArray.newArray(nodeCount, tracker);
    }

    @Override
    public WeightedDegreeCentrality compute() {
        nodeQueue.set(0);

        long batchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency);
        long threadSize = ParallelUtil.threadCount(batchSize, nodeCount);
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(formatWithLocale(
                "A concurrency of %d is too small to divide graph into at most Integer.MAX_VALUE tasks",
                concurrency
            ));
        }
        final List<Runnable> tasks = new ArrayList<>((int) threadSize);

        for (int i = 0; i < threadSize; i++) {
            tasks.add(new DegreeTask());
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        return this;
    }

    @Override
    public WeightedDegreeCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    private class DegreeTask implements Runnable {
        @Override
        public void run() {
            final MultiPartiteRelationshipIterator threadLocalGraph = graph.concurrentCopy();
            while (true) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                double[] weightedDegree = new double[1];
                threadLocalGraph.forEachRelationship(nodeId, Double.NaN, (sourceNodeId, targetNodeId, weight) -> {
                    if (weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    return true;
                });

                degrees.set(nodeId, weightedDegree[0]);

            }
        }
    }

    public HugeDoubleArray degrees() {
        return degrees;
    }

}
