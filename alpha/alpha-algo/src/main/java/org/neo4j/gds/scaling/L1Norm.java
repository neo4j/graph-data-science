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
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;

final class L1Norm extends ScalarScaler {

    final double l1Norm;

    private L1Norm(NodeProperties properties, double l1Norm) {
        super(properties);
        this.l1Norm = l1Norm;
    }

    static ScalarScaler initialize(NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor) {
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeSum(partition, properties)
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        var sum = tasks.stream().mapToDouble(ComputeSum::sum).sum();

        if (Math.abs(sum) < CLOSE_TO_ZERO) {
            return ZERO;
        } else {
            return new L1Norm(properties, sum);
        }
    }

    @Override
    public double scaleProperty(long nodeId) {
        return properties.doubleValue(nodeId) / l1Norm;
    }

    static class ComputeSum extends AggregatesComputer {
        
        private double sum;

        ComputeSum(Partition partition, NodeProperties property) {
            super(partition, property);
            this.sum = 0;
        }

        @Override
        void compute(long nodeId) {
            sum += Math.abs(properties.doubleValue(nodeId));
        }

        double sum() {
            return sum;
        }
    }

}
