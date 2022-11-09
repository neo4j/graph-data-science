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
package org.neo4j.gds.impl.spanningtree;

import org.neo4j.gds.utils.StringFormatting;

import java.util.List;
import java.util.Locale;
import java.util.function.DoubleUnaryOperator;

public class SpanningTreeCompanion {

    public static DoubleUnaryOperator parse(Object input) {
        if (input instanceof String) {
            var inputString = StringFormatting.toUpperCaseWithLocale((String) input);

            if (inputString.equals("MAXIMUM")) {
                return Prim.MAX_OPERATOR;
            } else if (inputString.equals("MINIMUM")) {
                return Prim.MIN_OPERATOR;
            }

            throw new IllegalArgumentException(String.format(
                Locale.getDefault(),
                "Input value `%s` for parameter `objective` is not supported. Must be one of: %s.",
                input,
                List.of("maximum", "minimum")
            ));
        } else if (input instanceof DoubleUnaryOperator) {
            var inputOperator = (DoubleUnaryOperator) input;
            if (inputOperator.equals(Prim.MAX_OPERATOR) || inputOperator.equals(Prim.MIN_OPERATOR)) {
                return inputOperator;

            }
            throw new IllegalArgumentException(String.format(
                Locale.getDefault(),
                "Input value for parameter `objective` is not supported. Must be one of: %s.",
                List.of("maximum", "minimum")
            ));
        }

        throw new IllegalArgumentException(StringFormatting.formatWithLocale(
            "Expected a String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(DoubleUnaryOperator doubleUnaryOperator) {

        if (doubleUnaryOperator.equals(Prim.MAX_OPERATOR)) {
            return "maximum";
        } else {
            return "minimum";
        }
    }
}
