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
package org.neo4j.gds.scaling.compute;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.progress.tracking.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class MinMaxAverageComputer {
    public record Result(double min, double max, double average) {}
    private MinMaxAverageComputer() {}

    public static Result compute(
        NodePropertyValues properties,
        long nodeCount,
        Concurrency concurrency,
        ProgressTracker progressTracker,
        ExecutorService executor
    ) {
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeMaxMinSum(partition, properties, progressTracker),
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
        var nodeCountOmittingMissingProperties = tasks.stream().mapToLong(AggregatesComputer::nodeCountOmittingMissingValues).sum();

        var avg = sum / nodeCountOmittingMissingProperties;

        return new Result(min, max, avg);
    }

    static class ComputeMaxMinSum extends AggregatesComputer {

        private double max;
        private double min;
        private double sum;

        ComputeMaxMinSum(Partition partition, NodePropertyValues property, ProgressTracker progressTracker) {
            super(partition, property, progressTracker);
            this.min = Double.MAX_VALUE;
            this.max = -Double.MAX_VALUE;
            this.sum = 0D;
        }

        @Override
        void compute(double propertyValue) {
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
