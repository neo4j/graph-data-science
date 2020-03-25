/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

public enum Aggregation {
    DEFAULT {
        public double merge(double runningTotal, double value) {
            throw new UnsupportedOperationException(
                    "This should never be used as a valid aggregation, " +
                    "just as a placeholder for the default aggregation of a given loader.");
        }
    },
    NONE {
        public double merge(double runningTotal, double value) {
            throw new UnsupportedOperationException(
                    "Multiple relationships between the same pair of nodes are not expected. " +
                    "Try using SKIP or some other aggregation.");
        }
    },
    SINGLE {
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
    },
    COUNT {
        public double merge(double runningTotal, double value) {
            return runningTotal + 1.0;
        }

        public double init(double firstValue, double secondValue) {
            return 2.0;
        }
    };

    public abstract double merge(double runningTotal, double value);
    public double init(double runningTotal, double value) {
        return merge(runningTotal, value);
    }

    public static Aggregation lookup(String name) {
        if (name.equalsIgnoreCase("SKIP")) {
            name = SINGLE.name();
        }
        try {
            return Aggregation.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            String availableStrategies = Arrays
                    .stream(Aggregation.values())
                    .map(Aggregation::name)
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException(String.format(
                    "Aggregation `%s` is not supported. Must be one of: %s.",
                    name,
                    availableStrategies));
        }
    }

    public static Aggregation parse(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return lookup(((String) object).toUpperCase());
        }
        if (object instanceof Aggregation) {
            return (Aggregation) object;
        }
        return null;
    }

}
