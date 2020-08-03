/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class DefaultValue {
    public static final DefaultValue DEFAULT = new DefaultValue(null);
    public static final long LONG_DEFAULT_FALLBACK = Long.MIN_VALUE;
    public static final double DOUBLE_DEFAULT_FALLBACK = Double.NaN;

    @Nullable
    private final Object defaultValue;

    public static DefaultValue of(Object defaultValue) {
        if (defaultValue instanceof DefaultValue) {
            return (DefaultValue) defaultValue;
        } else {
            return new DefaultValue(defaultValue);
        }
    }

    private DefaultValue(@Nullable Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    public long getLong() {
        if (defaultValue == null) {
            return LONG_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Double && Double.isNaN((double) defaultValue)) {
            return LONG_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Float && Float.isNaN((float) defaultValue)) {
            return LONG_DEFAULT_FALLBACK;
        }else if (defaultValue instanceof Number) {
            return ((Number) defaultValue).longValue();
        }
        throw getInvalidTypeException(Long.class);
    }

    public double getDouble() {
        if (defaultValue instanceof Long && defaultValue.equals(LONG_DEFAULT_FALLBACK)) {
            return DOUBLE_DEFAULT_FALLBACK;
        } else if (defaultValue instanceof Number) {
            return ((Number) defaultValue).doubleValue();
        } else if (defaultValue == null) {
            return DOUBLE_DEFAULT_FALLBACK;
        }
        throw getInvalidTypeException(Double.class);
    }

    public @Nullable Object getObject() {
        return defaultValue;
    }


    @Override
    public String toString() {
        return "DefaultValue(" + defaultValue + ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultValue that = (DefaultValue) o;
        return Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultValue);
    }

    @NotNull
    public ClassCastException getInvalidTypeException(Class<?> expectedClass) {
        return new ClassCastException(formatWithLocale("The default value %s cannot coerced into type %s.", defaultValue, expectedClass.getSimpleName()));
    }
}
