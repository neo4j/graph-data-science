/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * Wrapper around configuration options map
 */
public final class CypherMapWrapper {

    private final Map<String, Object> config;

    private CypherMapWrapper(Map<String, Object> config) {
        this.config = new HashMap<>(config);
    }

    /**
     * Checks if the given key exists in the configuration.
     *
     * @param key key to look for
     * @return true, iff the key exists
     */
    public boolean containsKey(String key) {
        return this.config.containsKey(key);
    }

    public Optional<String> getString(String key) {
        return Optional.ofNullable(getChecked(key, null, String.class));
    }

    public String requireString(String key) {
        return requireChecked(key, String.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        return getChecked(key, emptyMap(), Map.class);
    }

    // TODO: this is a special case as it's treating the empty string as null
    //       and doesn't throw on those values. Can we remove that specialization?

    /**
     * specialized getter for String which either returns the value
     * if found, the defaultValue if the key is not found or null if
     * the key is found but its value is empty.
     *
     * @param key          configuration key
     * @param defaultValue the default value if key is not found
     * @return the configuration value
     */
    @Contract("_, !null -> !null")
    public @Nullable String getString(String key, @Nullable String defaultValue) {
        String value = (String) config.getOrDefault(key, defaultValue);
        return (null == value || "".equals(value)) ? defaultValue : value;
    }

    @Contract("_, _, !null -> !null")
    public @Nullable String getString(String key, String oldKey, @Nullable String defaultValue) {
        String value = getChecked(key, null, String.class);
        if (value != null) {
            return value;
        }
        return getChecked(oldKey, defaultValue, String.class);
    }

    Optional<String> getStringWithFallback(String key, String oldKey) {
        Optional<String> value = getString(key);
        // #migration-note: On Java9+ there is a #or method on Optional that we should use instead
        //  https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/Optional.html#or(java.util.function.Supplier)
        if (!value.isPresent()) {
            value = getString(oldKey);
        }
        return value;
    }

    public boolean getBool(String key, boolean defaultValue) {
        return getChecked(key, defaultValue, Boolean.class);
    }

    public boolean requireBool(String key) {
        return requireChecked(key, Boolean.class);
    }

    public Number getNumber(String key, Number defaultValue) {
        return getChecked(key, defaultValue, Number.class);
    }

    public Number requireNumber(String key) {
        return requireChecked(key, Number.class);
    }

    public Number getNumber(String key, String oldKey, Number defaultValue) {
        Number value = getChecked(key, null, Number.class);
        if (value != null) {
            return value;
        }
        return getChecked(oldKey, defaultValue, Number.class);
    }

    public long getLong(String key, long defaultValue) {
        return getChecked(key, defaultValue, Long.class);
    }

    public long requireLong(String key) {
        return requireChecked(key, Long.class);
    }

    public int getInt(String key, int defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return getLongAsInt(key);
    }

    public int requireInt(String key) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return getLongAsInt(key);
    }

    private int getLongAsInt(String key) {
        Object value = config.get(key);
        // Cypher always uses longs, so we have to downcast them to ints
        if (value instanceof Long) {
            value = Math.toIntExact((Long) value);
        }
        return typedValue(key, Integer.class, value);
    }

    public double getDouble(String key, double defaultValue) {
        return getChecked(key, defaultValue, Double.class);
    }

    public double requireDouble(String key) {
        return requireChecked(key, Double.class);
    }

    /**
     * Get and convert the value under the given key to the given type.
     *
     * @return the found value under the key - if it is of the provided type,
     *     or the provided default value if no entry for the key is found (or it's mapped to null).
     * @throws IllegalArgumentException if a value was found, but it is not of the expected type.
     */
    @Contract("_, !null, _ -> !null")
    public @Nullable <V> V getChecked(String key, @Nullable V defaultValue, Class<V> expectedType) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return typedValue(key, expectedType, config.get(key));
    }

    public <V> V requireChecked(String key, Class<V> expectedType) {
        if (!containsKey(key)) {
            throw missingValueFor(key);
        }
        return typedValue(key, expectedType, config.get(key));
    }

    @SuppressWarnings("unchecked")
    @Contract("_, !null -> !null")
    @Deprecated
    public <V> @Nullable V get(String key, @Nullable V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return (V) value;
    }

    @SuppressWarnings("unchecked")
    @Contract("_, _, !null -> !null")
    @Deprecated
    public <V> @Nullable V get(String newKey, String oldKey, @Nullable V defaultValue) {
        Object value = config.get(newKey);
        if (null == value) {
            value = config.get(oldKey);
        }
        return null == value ? defaultValue : (V) value;
    }

    public static <T> T failOnNull(String key, T value) {
        if (value == null) {
            throw missingValueFor(key);
        }
        return value;
    }

    static <V> V typedValue(String key, Class<V> expectedType, @Nullable Object value) {
        if (!expectedType.isInstance(value)) {
            String message = String.format(
                "The value of `%s` must be of type `%s` but was `%s`.",
                key,
                expectedType.getSimpleName(),
                value == null ? "null" : value.getClass().getSimpleName()
            );
            throw new IllegalArgumentException(message);
        }
        return expectedType.cast(value);
    }

    private static IllegalArgumentException missingValueFor(String key) {
        return new IllegalArgumentException(String.format(
            "No value specified for the mandatory configuration parameter `%s`",
            key
        ));
    }

    // FACTORIES

    public static CypherMapWrapper create(Map<String, Object> config) {
        if (config == null) {
            return empty();
        }
        return new CypherMapWrapper(config);
    }

    public static CypherMapWrapper empty() {
        return new CypherMapWrapper(emptyMap());
    }

    CypherMapWrapper withString(String key, String value) {
        HashMap<String, Object> newMap = new HashMap<>(config);
        newMap.put(key, value);
        return new CypherMapWrapper(newMap);
    }
}
