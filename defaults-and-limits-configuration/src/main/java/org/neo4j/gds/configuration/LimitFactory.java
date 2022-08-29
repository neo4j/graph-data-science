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
package org.neo4j.gds.configuration;

import java.util.Locale;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LimitFactory {
    private LimitFactory() {}

    public static Limit create(Object value) {
        if (value instanceof Boolean) return new BooleanLimit((boolean) value);
        if (value instanceof Double) return new DoubleLimit((double) value);
        if (value instanceof Long) return new LongLimit((long) value);

        throw new IllegalArgumentException(formatWithLocale(
            "Unable to create limit for input value '%s' (%s)",
            value,
            value.getClass().getSimpleName().toLowerCase(Locale.ENGLISH)
        ));
    }
}
