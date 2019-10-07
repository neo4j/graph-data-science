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
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.PartitionedDoubleArrayResult;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

public class DegreeCentrality extends Algorithm<DegreeCentrality> {
    public static final double DEFAULT_WEIGHT = 0D;
    private final int nodeCount;
    private final boolean weighted;
    private final Direction direction;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;

    private long[] starts;
    private double[][] partitions;

    public DegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency,
            Direction direction,
            boolean weighted) {
        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        this.direction = direction;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.weighted = weighted;
    }

    public void compute() {
        int batchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency);
        int taskCount = ParallelUtil.threadCount(batchSize, nodeCount);
        final ArrayList<Runnable> tasks = new ArrayList<>(taskCount);

        this.starts = new long[taskCount];
        this.partitions = new double[taskCount][batchSize];

        long startNode = 0L;
        for (int i = 0; i < taskCount; i++) {
            starts[i] = startNode;
            if (weighted) {
                tasks.add(new WeightedDegreeTask(starts[i], partitions[i]));
            } else {
                tasks.add(new DegreeTask(starts[i], partitions[i]));
            }
            startNode += batchSize;
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);
    }

    public Algorithm<?> algorithm() {
        return this;
    }

    @Override
    public DegreeCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    public CentralityResult result() {
        return new PartitionedDoubleArrayResult(partitions, starts);
    }

    private class DegreeTask implements Runnable {
        private final long startNodeId;
        private final double[] partition;
        private final long endNodeId;

        DegreeTask(long start, double[] partition) {
            this.startNodeId = start;
            this.partition = partition;
            this.endNodeId = Math.min(start + partition.length, nodeCount);
        }

        @Override
        public void run() {
            for (long nodeId = startNodeId; nodeId < endNodeId && running(); nodeId++) {
                partition[Math.toIntExact(nodeId - startNodeId)] = graph.degree(nodeId, direction);
            }
        }
    }

    private class WeightedDegreeTask implements Runnable {
        private final long startNodeId;
        private final double[] partition;
        private final long endNodeId;

        WeightedDegreeTask(long start, double[] partition) {
            this.startNodeId = start;
            this.partition = partition;
            this.endNodeId = Math.min(start + partition.length, nodeCount);
        }

        @Override
        public void run() {
            for (long nodeId = startNodeId; nodeId < endNodeId && running(); nodeId++) {
                int index = Math.toIntExact(nodeId - startNodeId);
                graph.forEachRelationship(nodeId, direction, DEFAULT_WEIGHT, (sourceNodeId, targetNodeId, weight) -> {
                    if (weight > 0) {
                        partition[index] += weight;
                    }
                    return true;
                });
            }

        }
    }

}
