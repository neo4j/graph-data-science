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
package org.neo4j.graphalgo.core.utils;

import java.util.Locale;
import java.util.function.Function;

public final class GenericUtil {

    private GenericUtil() {}

    public static <T, U extends T> Function<T, U> castOrThrow(Class<U> expectedClazz) {
        return (from) -> castOrThrow(from, expectedClazz);
    }

    public static <T, U extends T> U castOrThrow(T from, Class<U> expectedClazz) {
        if (expectedClazz.isInstance(from)) {
            return expectedClazz.cast(from);
        }

        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "Expected %s, got %s.",
            expectedClazz.getSimpleName(),
            from == null ? "null" : from.getClass().getSimpleName()
        ));

    }
}
