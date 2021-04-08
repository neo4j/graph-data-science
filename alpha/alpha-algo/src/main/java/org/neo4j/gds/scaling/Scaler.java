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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface Scaler {

    double CLOSE_TO_ZERO = 1e-15;

    void scaleProperty(long nodeId, double[] result, int offset);

    default int dimension() {
        return 1;
    }

    Scaler ZERO_SCALER = (nodeId, result, offset) -> result[offset] = 0D;

    enum Variant {
        NONE {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return (nodeId, result, offset) -> result[offset] = properties.doubleValue(nodeId);
            }
        },
        MAX {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return Max.create(properties, nodeCount, concurrency, executor);
            }
        },
        MINMAX {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return MinMax.create(properties, nodeCount, concurrency, executor);
            }
        },
        MEAN {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return Mean.create(properties, nodeCount, concurrency, executor);
            }
        },
        LOG {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return LogTransformer.create(properties);
            }
        },
        STDSCORE {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return StdScore.create(properties, nodeCount, concurrency, executor);
            }
        },
        L1NORM {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return L1Norm.create(properties, nodeCount, concurrency, executor);
            }
        },
        L2NORM {
            @Override
            public Scaler create(
                NodeProperties properties, long nodeCount, int concurrency, ExecutorService executor
            ) {
                return L2Norm.create(properties, nodeCount, concurrency, executor);
            }
        };

        public static Variant lookup(String name) {
            try {
                return valueOf(name.toUpperCase(Locale.ENGLISH));
            } catch (IllegalArgumentException e) {
                String availableStrategies = Arrays
                    .stream(values())
                    .map(Variant::name)
                    .collect(Collectors.joining(", "));
                throw new IllegalArgumentException(formatWithLocale(
                    "Scaler `%s` is not supported. Must be one of: %s.",
                    name,
                    availableStrategies
                ));
            }
        }

        public static String toString(Variant variant) {
            return variant.name();
        }

        /**
         * Create a scaler. Some scalers rely on aggregate extreme values which are computed at construction time.
         */
        public abstract Scaler create(
            NodeProperties properties,
            long nodeCount,
            int concurrency,
            ExecutorService executor
        );
    }

    class ArrayScaler implements Scaler {

        private final List<Scaler> elementScalers;

        ArrayScaler(List<Scaler> elementScalers) {
            this.elementScalers = elementScalers;
        }

        @Override
        public void scaleProperty(long nodeId, double[] result, int offset) {
            for (int i = 0; i < dimension(); i++) {
                elementScalers.get(i).scaleProperty(nodeId, result, offset + i);
            }
        }

        @Override
        public int dimension() {
            return elementScalers.size();
        }
    }
}
