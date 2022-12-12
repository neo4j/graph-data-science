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
package org.neo4j.gds.core;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper around configuration options map
 */
public final class CypherMapWrapper implements CypherMapAccess {

    public static CypherMapWrapper create(@Nullable Map<String, Object> config) {
        if (config == null) {
            return empty();
        }
        Map<String, Object> configMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        config.forEach((key, value) -> {
            if (value != null) {
                configMap.put(key, value);
            }
        });
        return new CypherMapWrapper(configMap);
    }

    public static CypherMapWrapper empty() {
        return new CypherMapWrapper(Map.of());
    }

    private final Map<String, Object> config;

    private CypherMapWrapper(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public boolean containsKey(String key) {
        return this.config.containsKey(key);
    }

    @Override
    public Collection<String> keySet() {
        return Collections.unmodifiableSet(this.config.keySet());
    }

    @Override
    public int getLongAsInt(String key) {
        Object value = config.get(key);
        // Cypher always uses longs, so we have to downcast them to ints
        if (value instanceof Long) {
            value = Math.toIntExact((Long) value);
        }
        return typedValue(key, Integer.class, value);
    }

    @Override
    public <V> @NotNull V typedValue(String key, Class<V> expectedType) {
        return typedValue(key, expectedType, config.get(key));
    }

    private static <V> V typedValue(String key, Class<V> expectedType, @Nullable Object value) {
        if (Double.class.isAssignableFrom(expectedType) && value instanceof Number) {
            return expectedType.cast(((Number) value).doubleValue());
        } else if (expectedType.equals(Integer.class) && value instanceof Long) {
            return expectedType.cast(Math.toIntExact((Long) value));
        } else if (!expectedType.isInstance(value)) {
            String message = String.format(
                Locale.ENGLISH,
                "The value of `%s` must be of type `%s` but was `%s`.",
                key,
                expectedType.getSimpleName(),
                value == null ? "null" : value.getClass().getSimpleName()
            );
            throw new IllegalArgumentException(message);
        }
        return expectedType.cast(value);
    }

    @Override
    public Map<String, Object> toMap() {
        return new HashMap<>(config);
    }

    // FACTORIES

    public CypherMapWrapper withString(String key, String value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withNumber(String key, Number value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withBoolean(String key, Boolean value) {
        return withEntry(key, value);
    }

    public CypherMapWrapper withEntry(String key, Object value) {
        Map<String, Object> newMap = copyValues();
        newMap.put(key, value);
        return new CypherMapWrapper(newMap);
    }

    public CypherMapWrapper withoutEntry(String key) {
        if (!containsKey(key)) {
            return this;
        }
        Map<String, Object> newMap = copyValues();
        newMap.remove(key);
        return new CypherMapWrapper(newMap);
    }

    public CypherMapWrapper withoutAny(Collection<String> keys) {
        Map<String, Object> newMap = copyValues();
        newMap.keySet().removeAll(keys);
        return new CypherMapWrapper(newMap);
    }

    private Map<String, Object> copyValues() {
        Map<String, Object> copiedMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        copiedMap.putAll(config);
        return copiedMap;
    }
}
