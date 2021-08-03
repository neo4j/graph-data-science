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
package org.neo4j.graphalgo.beta.pregel;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.utils.StringJoining;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public enum Partitioning {
    RANGE,
    DEGREE,
    AUTO;

    private static final List<String> VALUES = Arrays
        .stream(Partitioning.values())
        .map(Partitioning::name)
        .collect(Collectors.toList());

    public static @Nullable Partitioning parse(Object input) {
        if (input instanceof String) {
            var inputString = ((String) input).toUpperCase(Locale.ENGLISH);

            if (!VALUES.contains(inputString)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Partitioning with name `%s` does not exist. Available options are %s.",
                    inputString,
                    StringJoining.join(VALUES)
                ));
            }

            return Partitioning.valueOf(inputString);
        }
        return (Partitioning) input;
    }

    public static String toString(Partitioning partitioning) {
        return partitioning.toString();
    }
}
