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
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.utils.StringJoining;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public abstract class ScalarScaler implements Scaler {

    protected final NodePropertyValues properties;

    protected ScalarScaler(NodePropertyValues properties) {this.properties = properties;}

    @Override
    public int dimension() {
        return 1;
    }

    public static final ScalarScaler ZERO = new ScalarScaler(null) {
        @Override
        public double scaleProperty(long nodeId) {
            return 0;
        }
    };

    public enum Variant {
        NONE {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return new ScalarScaler(properties) {
                    @Override
                    public double scaleProperty(long nodeId) {
                        return properties.doubleValue(nodeId);
                    }
                };
            }
        },
        MAX {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return Max.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        MINMAX {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return MinMax.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        MEAN {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return Mean.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        LOG {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return new LogScaler(properties);
            }
        },
        STDSCORE {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return StdScore.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        L1NORM {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return L1Norm.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        L2NORM {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return L2Norm.initialize(properties, nodeCount, concurrency, executor);
            }
        },
        CENTER {
            @Override
            public ScalarScaler create(
                NodePropertyValues properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return Center.initialize(properties, nodeCount, concurrency, executor);
            }
        };

        private static final List<String> VALUES = Arrays
            .stream(Variant.values())
            .map(Variant::name)
            .collect(Collectors.toList());

        public static Variant parse(Object name) {
            if (name instanceof String) {
                var inputString = toUpperCaseWithLocale((String) name);

                if (VALUES.contains(inputString)) {
                    return valueOf(inputString);
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Scaler `%s` is not supported. Expected one of: %s.",
                        name,
                        StringJoining.join(VALUES)
                    ));
                }
            } else if (name instanceof Variant) {
                return (Variant) name;
            }

            throw new IllegalArgumentException(formatWithLocale(
                "Unsupported scaler specified: `%s`. Expected one of: %s.",
                name,
                StringJoining.join(VALUES)
            ));
        }

        public static String toString(Variant variant) {
            return variant.name();
        }

        /**
         * Create a scaler. Some scalers rely on aggregate extreme values which are computed at construction time.
         */
        public abstract ScalarScaler create(
            NodePropertyValues properties,
            long nodeCount,
            int concurrency,
            ExecutorService executor
        );
    }

    abstract static class AggregatesComputer implements Runnable {

        private final Partition partition;
        final NodePropertyValues properties;

        AggregatesComputer(Partition partition, NodePropertyValues property) {
            this.partition = partition;
            this.properties = property;
        }

        @Override
        public void run() {
            long end = partition.startNode() + partition.nodeCount();
            for (long nodeId = partition.startNode(); nodeId < end; nodeId++) {
                compute(nodeId);
            }
        }

        abstract void compute(long nodeId);
    }
}
