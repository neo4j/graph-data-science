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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class Mean extends ScalarScaler {

    public static final String NAME = "mean";
    final double avg;
    final double maxMinDiff;

    private Mean(NodePropertyValues properties, double avg, double maxMinDiff) {
        super(properties);
        this.avg = avg;
        this.maxMinDiff = maxMinDiff;
    }

    @Override
    public double scaleProperty(long nodeId) {
        return (properties.doubleValue(nodeId) - avg) / maxMinDiff;
    }

    static ScalerFactory buildFrom(CypherMapWrapper mapWrapper) {
        mapWrapper.requireOnlyKeysFrom(List.of());
        return new ScalerFactory() {
            @Override
            public String name() {
                return NAME;
            }

            @Override
            public ScalarScaler create(
                NodePropertyValues properties,
                long nodeCount,
                int concurrency,
                ExecutorService executor
            ) {
                var tasks = PartitionUtils.rangePartition(
                    concurrency,
                    nodeCount,
                    partition -> new ComputeMaxMinSum(partition, properties),
                    Optional.empty()
                );
                RunWithConcurrency.builder()
                    .concurrency(concurrency)
                    .tasks(tasks)
                    .executor(executor)
                    .run();

                var min = tasks.stream().mapToDouble(ComputeMaxMinSum::min).min().orElse(Double.MAX_VALUE);
                var max = tasks.stream().mapToDouble(ComputeMaxMinSum::max).max().orElse(-Double.MAX_VALUE);
                var sum = tasks.stream().mapToDouble(ComputeMaxMinSum::sum).sum();

                var maxMinDiff1 = max - min;

                if (Math.abs(maxMinDiff1) < CLOSE_TO_ZERO) {
                    return ZERO;
                } else {
                    return new Mean(properties, sum / nodeCount, maxMinDiff1);
                }
            }
        };
    }

    static class ComputeMaxMinSum extends AggregatesComputer {

        private double max;
        private double min;
        private double sum;

        ComputeMaxMinSum(Partition partition, NodePropertyValues property) {
            super(partition, property);
            this.min = Double.MAX_VALUE;
            this.max = -Double.MAX_VALUE;
            this.sum = 0D;
        }

        @Override
        void compute(long nodeId) {
            var propertyValue = properties.doubleValue(nodeId);
            sum += propertyValue;
            if (propertyValue < min) {
                min = propertyValue;
            }
            if (propertyValue > max) {
                max = propertyValue;
            }
        }

        double max() {
            return max;
        }

        double min() {
            return min;
        }

        double sum() {
            return sum;
        }
    }

}
