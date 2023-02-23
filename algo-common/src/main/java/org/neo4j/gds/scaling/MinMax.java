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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class MinMax extends ScalarScaler {

    public static final String NAME = "minmax";

    final double min;
    final double maxMinDiff;

    private MinMax(NodePropertyValues properties, double min, double maxMinDiff) {
        super(properties);
        this.min = min;
        this.maxMinDiff = maxMinDiff;
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
                ProgressTracker progressTracker,
                ExecutorService executor
            ) {
                var tasks = PartitionUtils.rangePartition(
                    concurrency,
                    nodeCount,
                    partition -> new ComputeMaxMin(partition, properties, progressTracker),
                    Optional.empty()
                );
                RunWithConcurrency.builder()
                    .concurrency(concurrency)
                    .tasks(tasks)
                    .executor(executor)
                    .run();

                var min = tasks.stream().mapToDouble(ComputeMaxMin::min).min().orElse(Double.MAX_VALUE);
                var max = tasks.stream().mapToDouble(ComputeMaxMin::max).max().orElse(-Double.MAX_VALUE);

                var maxMinDiff = max - min;

                if (Math.abs(maxMinDiff) < CLOSE_TO_ZERO) {
                    return ZERO;
                } else {
                    return new MinMax(properties, min, maxMinDiff);
                }
            }
        };
    }

    @Override
    public double scaleProperty(long nodeId) {
        return (properties.doubleValue(nodeId) - min) / maxMinDiff;
    }

    static class ComputeMaxMin extends AggregatesComputer {

        private double min;
        private double max;

        ComputeMaxMin(Partition partition, NodePropertyValues property, ProgressTracker progressTracker) {
            super(partition, property, progressTracker);
            this.min = Double.MAX_VALUE;
            this.max = -Double.MAX_VALUE;
        }

        @Override
        void compute(long nodeId) {
            var propertyValue = properties.doubleValue(nodeId);
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
    }

}
