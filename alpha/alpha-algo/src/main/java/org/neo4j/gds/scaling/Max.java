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

final class Max extends Scaler.ScalarScaler {

    final double maxAbs;

    private Max(NodeProperties properties, double maxAbs) {
        super(properties);
        this.maxAbs = maxAbs;
    }

    static ScalarScaler create(NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor) {
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeAbsMax(partition, properties)
        );

        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        var absMax = tasks.stream().mapToDouble(ComputeAbsMax::absMax).max().orElse(0);

        if (Math.abs(absMax) < CLOSE_TO_ZERO) {
            return ZERO;
        } else {
            return new Max(properties, absMax);
        }
    }

    @Override
    public double scaleProperty(long nodeId) {
        return properties.doubleValue(nodeId) / maxAbs;
    }

    static class ComputeAbsMax extends AggregatesComputer {

        private double absMax;

        ComputeAbsMax(Partition partition, NodeProperties property) {
            super(partition, property);
            this.absMax = 0;
        }

        @Override
        void compute(long nodeId) {
            var absoluteValue = Math.abs(properties.doubleValue(nodeId));
            if (absoluteValue > absMax) {
                absMax = absoluteValue;
            }
        }

        double absMax() {
            return absMax;
        }
    }

}
