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
package org.neo4j.graphalgo;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public enum Orientation {

    NATURAL,
    REVERSE,
    UNDIRECTED;

    public static Orientation of(String value) {
        try {
            return Orientation.valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            String availableProjections = Arrays
                .stream(Orientation.values())
                .map(Orientation::name)
                .collect(Collectors.joining(", "));
            throw new IllegalArgumentException(formatWithLocale(
                "Orientation `%s` is not supported. Must be one of: %s.",
                value,
                availableProjections));
        }
    }

    public static Orientation parse(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return of(((String) object).toUpperCase(Locale.ENGLISH));
        }
        if (object instanceof Orientation) {
            return (Orientation) object;
        }
        return null;
    }

    public static String toString(Orientation orientation) {
        return orientation.toString();
    }
}
