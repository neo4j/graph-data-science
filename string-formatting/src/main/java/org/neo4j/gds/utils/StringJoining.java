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
package org.neo4j.gds.utils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class StringJoining {

    private StringJoining() {}

    public static String join(Stream<String> alternatives) {
        return join(alternatives.collect(Collectors.toList()));
    }

    public static String join(Stream<String> alternatives, CharSequence delimiter) {
        return join(alternatives.collect(Collectors.toList()), delimiter);
    }

    public static String join(Collection<String> alternatives) {
        if (alternatives.isEmpty()) {
            return "[]";
        }
        return join(alternatives, "', '", "['", "']");
    }

    public static String join(Collection<String> alternatives, CharSequence delimiter) {
        return joinInGivenOrder(alternatives.stream().sorted(), delimiter);
    }

    public static String joinInGivenOrder(Stream<String> alternatives, CharSequence delimiter) {
        return alternatives.collect(joining(delimiter));
    }

    public static String join(
        Collection<String> alternatives,
        CharSequence delimiter,
        CharSequence prefix,
        CharSequence suffix
    ) {
        return join(alternatives.stream(), delimiter, prefix, suffix);
    }

    public static String join(
        Stream<String> alternatives,
        CharSequence delimiter,
        CharSequence prefix,
        CharSequence suffix
    ) {
        return alternatives.sorted().collect(joining(delimiter, prefix, suffix));
    }

    public static String joinVerbose(Collection<String> alternatives) {
        if (alternatives.size() == 1) {
            return join(alternatives, ", ");
        }
        if (alternatives.size() == 2) {
            return join(alternatives, " and ");
        }
        return joinVerbose(List.of(
            join(alternatives.stream().limit(alternatives.size() - 1), ", "),
            alternatives.stream().skip(alternatives.size() - 1).findFirst().orElseThrow(() -> new IllegalStateException("No alternatives found."))
        ));
    }
}
