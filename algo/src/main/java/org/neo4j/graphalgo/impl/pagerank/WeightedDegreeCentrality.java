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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class WeightedDegreeCentrality extends Algorithm<WeightedDegreeCentrality> {
    private final long nodeCount;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    private HugeDoubleArray degrees;
    private HugeObjectArray<HugeDoubleArray> weights;
    private AllocationTracker tracker;

    public WeightedDegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency,
            AllocationTracker tracker
    ) {
        if (!graph.hasRelationshipProperty()) {
            throw new UnsupportedOperationException("WeightedDegreeCentrality requires a weight property to be loaded.");
        }

        this.tracker = tracker;
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_CONCURRENCY;
        }

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        degrees = HugeDoubleArray.newArray(nodeCount, tracker);
        weights = HugeObjectArray.newArray(HugeDoubleArray.class, nodeCount, tracker);
    }

    public WeightedDegreeCentrality compute(boolean cacheWeights) {
        nodeQueue.set(0);

        long batchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency);
        long threadSize = ParallelUtil.threadCount(batchSize, nodeCount);
        if (threadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format(
                    "A concurrency of %d is too small to divide graph into at most Integer.MAX_VALUE tasks",
                    concurrency));
        }
        final List<Runnable> tasks = new ArrayList<>((int) threadSize);

        for (int i = 0; i < threadSize; i++) {
            if(cacheWeights) {
                tasks.add(new CacheDegreeTask());
            } else {
                tasks.add(new DegreeTask());
            }
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
            Direction loadDirection = graph.getLoadDirection();
            while (true) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                double[] weightedDegree = new double[1];
                graph.forEachRelationship(nodeId, loadDirection, Double.NaN, (sourceNodeId, targetNodeId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    return true;
                });

                degrees.set(nodeId, weightedDegree[0]);

            }
        }
    }

    private class CacheDegreeTask implements Runnable {
        @Override
        public void run() {
            Direction loadDirection = graph.getLoadDirection();
            double[] weightedDegree = new double[1];
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                final HugeDoubleArray nodeWeights = HugeDoubleArray.newArray(graph.degree(nodeId, loadDirection), tracker);
                weights.set(nodeId, nodeWeights);

                int[] index = {0};
                weightedDegree[0] = 0D;
                graph.forEachRelationship(nodeId, loadDirection, Double.NaN, (sourceNodeId, targetNodeId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    nodeWeights.set(index[0], weight);
                    index[0]++;
                    return true;
                });

                degrees.set(nodeId, weightedDegree[0]);

            }
        }
    }

    public HugeDoubleArray degrees() {
        return degrees;
    }
    public HugeObjectArray<HugeDoubleArray> weights() {
        return weights;
    }

}
