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
package org.neo4j.gds;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public enum Orientation {

    NATURAL {
        @Override
        public Orientation inverse() {
            return REVERSE;
        }
    },
    REVERSE {
        @Override
        public Orientation inverse() {
            return NATURAL;
        }
    },
    UNDIRECTED {
        @Override
        public Orientation inverse() {
            return UNDIRECTED;
        }
    };

    public abstract Orientation inverse();

    private static final List<String> VALUES = Arrays
        .stream(Orientation.values())
        .map(Orientation::name)
        .collect(Collectors.toList());

    public static Orientation parse(Object input) {
        if (input instanceof String) {
            var inputString = ((String) input).toUpperCase(Locale.ENGLISH);
            if (VALUES.contains(inputString)) {
                return Orientation.valueOf(inputString);
            }

            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Orientation `%s` is not supported. Must be one of: %s.",
                inputString,
                VALUES
            ));
        }
        else if (input instanceof Orientation) {
            return (Orientation) input;
        }

        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Expected Orientation or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(Orientation orientation) {
        return orientation.toString();
    }
}
