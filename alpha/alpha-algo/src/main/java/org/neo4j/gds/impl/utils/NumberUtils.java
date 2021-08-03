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
package org.neo4j.gds.impl.utils;

import java.util.Optional;

public final class NumberUtils {
    public static double getDoubleValue(Number value) {
        return Optional.ofNullable(value).map(Number::doubleValue).orElse(Double.NaN);
    }

    public static long getLongValue(Number value) throws IllegalArgumentException {
        return Optional.ofNullable(value).map(Number::longValue).orElseThrow(
            () -> new IllegalArgumentException("Null cannot be converted to long")
        );
    }
}
