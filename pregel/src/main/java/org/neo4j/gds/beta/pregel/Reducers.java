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
package org.neo4j.gds.beta.pregel;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public final class Reducers {
    private Reducers() {}

    public static Reducer parse(Object reducer) {
        if (reducer instanceof String s) {
            return switch (toUpperCaseWithLocale(s)) {
                case "SUM" -> new Reducer.Sum();
                case "MIN" -> new Reducer.Min();
                case "MAX" -> new Reducer.Max();
                case "COUNT" -> new Reducer.Count();
                default -> throw new IllegalArgumentException(formatWithLocale("Unknown reducer: `%s`", reducer));
            };
        }
        if (reducer instanceof Reducer r) {
            return r;
        }
        throw new IllegalArgumentException(formatWithLocale("Unknown reducer: `%s`", reducer));
    }

    public static String toString(Reducer reducer) {
        return toUpperCaseWithLocale(reducer.getClass().getSimpleName());
    }
}
