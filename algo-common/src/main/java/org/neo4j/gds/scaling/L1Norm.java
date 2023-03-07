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

public final class L1Norm extends ScalarScaler {

    public static final String TYPE = "l1norm";
    final double l1Norm;

    private L1Norm(NodePropertyValues properties, double l1Norm) {
        super(properties, Map.of());
        this.l1Norm = l1Norm;
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
                    partition -> new ComputeAbsoluteSum(partition, properties, progressTracker),
                    Optional.empty()
                );
                RunWithConcurrency.builder()
                    .concurrency(concurrency)
                    .tasks(tasks)
                    .executor(executor)
                    .run();

                var absoluteSum = tasks.stream().mapToDouble(ComputeAbsoluteSum::sum).sum();

                if (absoluteSum < CLOSE_TO_ZERO) {
                    return ZERO;
                } else {
                    return new L1Norm(properties, absoluteSum);
                }
            }
        };
    }

    @Override
    public double scaleProperty(long nodeId) {
        return properties.doubleValue(nodeId) / l1Norm;
    }

    static class ComputeAbsoluteSum extends AggregatesComputer {

        private double sum;

        ComputeAbsoluteSum(Partition partition, NodePropertyValues property, ProgressTracker progressTracker) {
            super(partition, property, progressTracker);
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
