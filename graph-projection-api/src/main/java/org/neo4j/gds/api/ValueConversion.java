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
package org.neo4j.gds.api;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ValueConversion {

    private ValueConversion() {}

    public static long exactDoubleToLong(double d) throws UnsupportedOperationException {
        if (d % 1 == 0) {
            return (long) d;
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot safely convert %.2f into an long value",
                d
            ));
        }
    }

    public static double exactLongToDouble(long l) {
        if (l <= 1L << 53) {
            return (double) l;
        } else {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot safely convert %d into an double value",
                l
            ));
        }
    }

    public static float exactLongToFloat(long l) {
        // Math.ulp() tells us that integer precision for float is > 1.0
        // for the values checked for below.
        if (l >= 1L << 24 || l <= -(1L << 24)) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot safely convert %d into a float value",
                l
            ));
        }

        return (float) l;
    }

    public static float notOverflowingDoubleToFloat(double d) {
        if (d > Float.MAX_VALUE || d < -Float.MAX_VALUE) {
            throw new UnsupportedOperationException(formatWithLocale(
                "Cannot safely convert %.2f into a float value",
                d
            ));
        }

        return (float) d;
    }
}
