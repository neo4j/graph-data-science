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
package org.neo4j.gds.api.nodeproperties;

import java.util.OptionalInt;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * The textual form of a {@link ValueType} as it appears in the graph-store formats, for example in a CSV
 * header ({@code embedding:float_vector(128)}) or in the JSON metadata document.
 *
 *
 */
public record ValueTypeToken(ValueType valueType, OptionalInt dimension) {

    private static final char DIMENSION_OPEN = '(';
    private static final char DIMENSION_CLOSE = ')';

    public static ValueTypeToken of(ValueType valueType) {
        return new ValueTypeToken(valueType, OptionalInt.empty());
    }

    public static ValueTypeToken of(ValueType valueType, int dimension) {
        return new ValueTypeToken(valueType, OptionalInt.of(dimension));
    }

    /**
     * @return the token for a non-vector type, or for a vector type whose dimension is not known here
     *         (the property schema does not carry one).
     */
    public static String format(ValueType valueType) {
        return valueType.csvName();
    }

    /**
     * @return {@code float_vector(128)} for a vector type with a known dimension, otherwise the plain
     *         {@link ValueType#csvName()}.
     */
    public static String format(ValueType valueType, OptionalInt dimension) {
        if (!valueType.isVector() || dimension.isEmpty()) {
            return format(valueType);
        }
        return valueType.csvName() + DIMENSION_OPEN + dimension.getAsInt() + DIMENSION_CLOSE;
    }

    public static ValueTypeToken parse(String token) {
        var open = token.indexOf(DIMENSION_OPEN);
        if (open < 0) {
            return of(ValueType.fromCsvName(token));
        }

        if (token.charAt(token.length() - 1) != DIMENSION_CLOSE) {
            throw new IllegalArgumentException(formatWithLocale(
                "Value type `%s` is missing the closing `%s` of its dimension",
                token,
                DIMENSION_CLOSE
            ));
        }

        var valueType = ValueType.fromCsvName(token.substring(0, open));
        if (!valueType.isVector()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Value type `%s` does not take a dimension",
                valueType.csvName()
            ));
        }

        var dimensionText = token.substring(open + 1, token.length() - 1);
        int dimension;
        try {
            dimension = Integer.parseInt(dimensionText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(formatWithLocale(
                "Value type `%s` has a dimension that is not a number: `%s`",
                token,
                dimensionText
            ), e);
        }
        if (dimension <= 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Value type `%s` must have a positive dimension, but got: %d",
                token,
                dimension
            ));
        }

        return of(valueType, dimension);
    }
}
