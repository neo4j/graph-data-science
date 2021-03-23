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

        // calculate sum and squared sum
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeSums(partition.startNode(), partition.nodeCount(), properties)
        );
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        // calculate global metrics
        var squaredSum = tasks.stream().mapToDouble(ComputeSums::squaredSum).sum();
        var sum = tasks.stream().mapToDouble(ComputeSums::sum).sum();
        var avg = sum / nodeCount;
        var variance = (squaredSum - 2 * avg * sum + nodeCount * avg * avg) / nodeCount;
        var std = Math.sqrt(variance);

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

    static class ComputeSums implements Runnable {

        private final long start;
        private final long length;
        private final NodeProperties properties;
        private double squaredSum;
        private double sum;

        ComputeSums(long start, long length, NodeProperties property) {
            this.start = start;
            this.length = length;
            this.properties = property;
            this.squaredSum = 0D;
            this.sum = 0D;
        }

        @Override
        public void run() {
            for (long nodeId = start; nodeId < (start + length); nodeId++) {
                double propertyValue = properties.doubleValue(nodeId);
                this.sum += propertyValue;
                this.squaredSum += propertyValue * propertyValue;
            }
        }

        double squaredSum() {
            return squaredSum;
        }

        double sum() {
            return sum;
        }
    }

}
