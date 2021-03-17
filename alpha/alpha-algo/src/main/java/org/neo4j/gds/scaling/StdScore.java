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
package org.neo4j.gds.scaling;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;

final class StdScore implements Scaler {

    private final NodeProperties properties;
    final double avg;
    final double std;

    private StdScore(NodeProperties properties, double avg, double std) {
        this.properties = properties;
        this.avg = avg;
        this.std = std;
    }

    static Scaler create(NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor) {
        if (nodeCount == 0) {
            return new StdScore(properties, 0, 0);
        }

        // calculate mean
        var sumTasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeSum(partition.startNode(), partition.nodeCount(), properties)
        );

        ParallelUtil.runWithConcurrency(concurrency, sumTasks, executor);

        // calculate stdDev
        var avg = sumTasks.stream().mapToDouble(ComputeSum::result).sum() / nodeCount;

        var squaredSumTasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeSquareSum(partition.startNode(), partition.nodeCount(), properties, avg)
        );

        ParallelUtil.runWithConcurrency(concurrency, squaredSumTasks, executor);


        var squaredSum = squaredSumTasks.stream().mapToDouble(ComputeSquareSum::result).sum();
        var std = Math.sqrt(squaredSum / nodeCount);

        if (Math.abs(std) < CLOSE_TO_ZERO) {
            return ZERO_SCALER;
        } else {
            return new StdScore(properties, avg, std);
        }
    }

    @Override
    public double scaleProperty(long nodeId) {
        return (properties.doubleValue(nodeId) - avg) / std;
    }

    static class ComputeSum implements Runnable {

        private final long start;
        private final long length;
        private final NodeProperties properties;
        private double sum;

        ComputeSum(long start, long length, NodeProperties property) {
            this.start = start;
            this.length = length;
            this.properties = property;
            this.sum = 0D;
        }

        @Override
        public void run() {
            for (long nodeId = start; nodeId < (start + length); nodeId++) {
                sum += properties.doubleValue(nodeId);
            }
        }

        double result() {
            return sum;
        }
    }

    // sum((x-avg)^2)
    static class ComputeSquareSum implements Runnable {

        private final long start;
        private final long length;
        private final NodeProperties properties;
        private final double avg;
        private double aggregate;

        ComputeSquareSum(long start, long length, NodeProperties property, double avg) {
            this.start = start;
            this.length = length;
            this.properties = property;
            this.avg = avg;
            this.aggregate = 0D;
        }

        @Override
        public void run() {
            for (long nodeId = start; nodeId < (start + length); nodeId++) {
                aggregate += Math.pow(properties.doubleValue(nodeId) - avg, 2);
            }
        }

        double result() {
            return aggregate;
        }
    }

}
