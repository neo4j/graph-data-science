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

final class Center extends ScalarScaler {

    static final String NAME = "center";
    final double avg;

    private Center(NodePropertyValues properties, double avg) {
        super(properties);
        this.avg = avg;
    }

    static ScalarScaler initialize(
        NodePropertyValues properties,
        long nodeCount,
        int concurrency,
        ExecutorService executor
    ) {
        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ComputeSum(partition, properties),
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executor)
            .run();
        var sum = tasks.stream().mapToDouble(ComputeSum::sum).sum();
        var avg = sum / nodeCount;

        return new Center(properties, avg);

    }

    @Override
    public double scaleProperty(long nodeId) {
        return (properties.doubleValue(nodeId) - avg);
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
                return initialize(properties, nodeCount, concurrency, executor);
            }
        };
    }

    static class ComputeSum extends AggregatesComputer {

        private double sum;

        ComputeSum(Partition partition, NodePropertyValues property) {
            super(partition, property);
            this.sum = 0D;
        }

        @Override
        void compute(long nodeId) {
            double propertyValue = properties.doubleValue(nodeId);
            this.sum += propertyValue;
        }

        double sum() {
            return sum;
        }
    }

}
