/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum DeduplicationStrategy {
    DEFAULT {
        public double merge(double runningTotal, double value) {
            throw new UnsupportedOperationException(
                    "This should never be used as a deduplication strategy, " +
                    "just as a placeholder for the default strategy of a given loader.");
        }
    },
    NONE {
        public double merge(double runningTotal, double value) {
            throw new UnsupportedOperationException(
                    "Multiple relationships between the same pair of nodes are not expected. " +
                    "Try using SKIP or some other duplicate relationships strategy.");
        }
    },
    SKIP {
        public double merge(double runningTotal, double value) {
            return runningTotal;
        }
    },
    SUM {
        public double merge(double runningTotal, double value) {
            return runningTotal + value;
        }
    },
    MIN {
        public double merge(double runningTotal, double value) {
            return Math.min(runningTotal, value);
        }
    },
    MAX {
        public double merge(double runningTotal, double value) {
            return Math.max(runningTotal, value);
        }
    };

    public abstract double merge(double runningTotal, double value);

    public static DeduplicationStrategy lookup(String name) {
        DeduplicationStrategy deduplicationStrategy = null;
        try {
            deduplicationStrategy = DeduplicationStrategy.valueOf(name);
        } catch (IllegalArgumentException e) {
            String availableStrategies = Arrays
                    .stream(DeduplicationStrategy.values())
                    .map(DeduplicationStrategy::name)
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException(String.format(
                    "Deduplication strategy `%s` is not supported. Must be one of: %s.",
                    name,
                    availableStrategies));
        }
        return deduplicationStrategy;
    }

}
