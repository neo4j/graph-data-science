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

final class L2Norm implements Scaler {

    private final NodeProperties properties;
    final double euclideanLength;

    private L2Norm(NodeProperties properties, double euclideanLength) {
        this.properties = properties;
        this.euclideanLength = euclideanLength;
    }

    static Scaler create(NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor) {
        if (nodeCount == 0) {
            return new L2Norm(properties, 0);
        }

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeAggregate(partition.startNode(), partition.nodeCount(), properties)
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        var squaredSum = tasks.stream().mapToDouble(ComputeAggregate::squaredSum).sum();
        var euclideanLength = Math.sqrt(squaredSum);

        if (Math.abs(euclideanLength) < CLOSE_TO_ZERO) {
            return ZERO_SCALER;
        } else {
            return new L2Norm(properties, euclideanLength);
        }
    }

    @Override
    public void scaleProperty(long nodeId, double[] result, int offset) {
        result[offset] = properties.doubleValue(nodeId) / euclideanLength;
    }

    static class ComputeAggregate implements Runnable {

        private final long start;
        private final long length;
        private final NodeProperties properties;
        private double squaredSum;

        ComputeAggregate(long start, long length, NodeProperties property) {
            this.start = start;
            this.length = length;
            this.properties = property;
            this.squaredSum = 0D;
        }

        @Override
        public void run() {
            for (long nodeId = start; nodeId < (start + length); nodeId++) {
                var propertyValue = properties.doubleValue(nodeId);
                squaredSum += propertyValue * propertyValue;
            }
        }

        double squaredSum() {
            return squaredSum;
        }
    }

}
