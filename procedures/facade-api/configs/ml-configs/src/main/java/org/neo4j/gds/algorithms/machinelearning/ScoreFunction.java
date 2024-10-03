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
package org.neo4j.gds.algorithms.machinelearning;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public enum ScoreFunction {
    TRANSE,
    DISTMULT;

    private static final List<String> VALUES = Arrays
        .stream(ScoreFunction.values())
        .map(ScoreFunction::name)
        .collect(Collectors.toList());

    public static ScoreFunction parse(Object input) {
        if (input instanceof String) {
            var inputString = toUpperCaseWithLocale((String) input);

            if (VALUES.contains(inputString)) {
                return ScoreFunction.valueOf(inputString.toUpperCase(Locale.ENGLISH));
            }

            throw new IllegalArgumentException(formatWithLocale(
                "Score function `%s` is not supported. Must be one of: %s.",
                inputString,
                VALUES
            ));
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Score function `%s` is not supported. Must be one of: %s.",
                input,
                VALUES
            ));
        }
    }

    public static String toString(ScoreFunction scoreFunction) {
        return scoreFunction.toString();
    }

}
