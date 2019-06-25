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
package org.neo4j.graphalgo.impl.degree;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.DoubleArrayResult;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WeightedDegreeCentrality extends Algorithm<WeightedDegreeCentrality> implements DegreeCentralityAlgorithm {
    private final int nodeCount;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicInteger nodeQueue = new AtomicInteger();

    private double[] degrees;
    private double[][] weights;

    public WeightedDegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency
    ) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_CONCURRENCY;
        }

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        nodeCount = Math.toIntExact(graph.nodeCount());
        degrees = new double[nodeCount];
        weights = new double[nodeCount][];
    }

    public WeightedDegreeCentrality compute(boolean cacheWeights) {
        nodeQueue.set(0);

        int batchSize = ParallelUtil.adjustBatchSize(nodeCount, concurrency);
        int taskCount = ParallelUtil.threadSize(batchSize, nodeCount);
        final ArrayList<Runnable> tasks = new ArrayList<>(taskCount);

        for (int i = 0; i < taskCount; i++) {
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
    public WeightedDegreeCentrality release() {
        graph = null;
        return null;
    }

    @Override
    public CentralityResult result() {
        return new DoubleArrayResult(degrees);
    }

    @Override
    public void compute() {
        compute(false);
    }

    @Override
    public Algorithm<?> algorithm() {
        return this;
    }

    private class DegreeTask implements Runnable {
        @Override
        public void run() {
            Direction loadDirection = graph.getLoadDirection();
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                double[] weightedDegree = new double[1];
                graph.forEachRelationship(nodeId, loadDirection, (sourceNodeId, targetNodeId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    return true;
                });

                degrees[nodeId] = weightedDegree[0];

            }
        }
    }

    private class CacheDegreeTask implements Runnable {
        @Override
        public void run() {
            Direction loadDirection = graph.getLoadDirection();
            for (; ; ) {
                final int nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                weights[nodeId] = new double[graph.degree(nodeId, loadDirection)];

                int[] index = {0};
                double[] weightedDegree = new double[1];
                graph.forEachRelationship(nodeId, loadDirection, (sourceNodeId, targetNodeId, weight) -> {
                    if(weight > 0) {
                        weightedDegree[0] += weight;
                    }

                    weights[nodeId][index[0]] = weight;
                    index[0]++;
                    return true;
                });

                degrees[nodeId] = weightedDegree[0];

            }
        }
    }

    public double[] degrees() {
        return degrees;
    }
    public double[][] weights() {
        return weights;
    }

    public Stream<DegreeCentrality.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new DegreeCentrality.Result(graph.toOriginalNodeId(nodeId), degrees[nodeId]));
    }

}
