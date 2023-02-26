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

public final class Max extends ScalarScaler {

    public static final String NAME = "max";
    final double maxAbs;

    private Max(NodePropertyValues properties, double maxAbs) {
        super(properties);
        this.maxAbs = maxAbs;
    }

    @Override
    public double scaleProperty(long nodeId) {
        return properties.doubleValue(nodeId) / maxAbs;
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
                    partition -> new ComputeAbsMax(partition, properties),
                    Optional.empty()
                );
                RunWithConcurrency.builder()
                    .concurrency(concurrency)
                    .tasks(tasks)
                    .executor(executor)
                    .run();

                var absMax = tasks.stream().mapToDouble(ComputeAbsMax::absMax).max().orElse(0);

                if (Math.abs(absMax) < CLOSE_TO_ZERO) {
                    return ZERO;
                } else {
                    return new Max(properties, absMax);
                }
            }
        };
    }

    static class ComputeAbsMax extends AggregatesComputer {

        private double absMax;

        ComputeAbsMax(Partition partition, NodePropertyValues property) {
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
