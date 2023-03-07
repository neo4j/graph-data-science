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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

final class StdScore extends ScalarScaler {

    static final String TYPE = "stdscore";
    final double avg;
    final double std;

    private StdScore(NodePropertyValues properties, Map<String, List<Double>> statistics, double avg, double std) {
        super(properties, statistics);
        this.avg = avg;
        this.std = std;
    }

    @Override
    public double scaleProperty(long nodeId) {
        return (properties.doubleValue(nodeId) - avg) / std;
    }

    static ScalerFactory buildFrom(CypherMapWrapper mapWrapper) {
        mapWrapper.requireOnlyKeysFrom(List.of());
        return new ScalerFactory() {
            @Override
            public String type() {
                return TYPE;
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
                    partition -> new ComputeSumAndSquaredSum(partition, properties, progressTracker),
                    Optional.empty()
                );
                RunWithConcurrency.builder()
                    .concurrency(concurrency)
                    .tasks(tasks)
                    .executor(executor)
                    .run();

                // calculate global metrics
                var squaredSum = tasks.stream().mapToDouble(ComputeSumAndSquaredSum::squaredSum).sum();
                var sum = tasks.stream().mapToDouble(ComputeSumAndSquaredSum::sum).sum();
                var avg = sum / nodeCount;
                // std = σ² = Σ(pᵢ - avg)² / N =
                // (Σ(pᵢ²) + Σ(avg²) - 2avgΣ(pᵢ)) / N =
                // (Σ(pᵢ²) + Navg² - 2avgΣ(pᵢ)) / N =
                // (Σ(pᵢ²) + avg(Navg - 2Σ(pᵢ)) / N
                var variance = (squaredSum + avg * (nodeCount * avg - 2 * sum)) / nodeCount;
                var std = Math.sqrt(variance);

                var statistics = Map.of(
                    "avg", List.of(avg),
                    "std", List.of(std)
                );

                if (Math.abs(std) < CLOSE_TO_ZERO) {
                    return new StatsOnly(statistics);
                } else {
                    return new StdScore(properties, statistics, avg, std);
                }
            }
        };
    }

    static class ComputeSumAndSquaredSum extends AggregatesComputer {

        private double squaredSum;
        private double sum;

        ComputeSumAndSquaredSum(Partition partition, NodePropertyValues property, ProgressTracker progressTracker) {
            super(partition, property, progressTracker);
            this.squaredSum = 0D;
            this.sum = 0D;
        }

        @Override
        void compute(long nodeId) {
            double propertyValue = properties.doubleValue(nodeId);
            this.sum += propertyValue;
            this.squaredSum += propertyValue * propertyValue;
        }

        double squaredSum() {
            return squaredSum;
        }

        double sum() {
            return sum;
        }
    }

}
