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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
        if (config.containsKey(key)) {
            // Optional.of will throw an NPE if key does not exist because of the default value
            //  which we want to have as a kind of sanity check - the default value *should* not be used
            return Optional.of(getChecked(key, null, String.class));
        }
        return Optional.empty();
    }

    /**
     * specialized getter for String which either returns the value
     * if found, the defaultValue if the key is not found or null if
     * the key is found but its value is empty.
     *
     * @param key          configuration key
     * @param defaultValue the default value if key is not found
     * @return the configuration value
     */
    public String getString(String key, String defaultValue) {
        String value = (String) config.getOrDefault(key, defaultValue);
        return (null == value || "".equals(value)) ? defaultValue : value;
    }

    public String getString(String key, String oldKey, String defaultValue) {
        return getChecked(key, oldKey, defaultValue, String.class);
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

    Boolean getBool(String key, boolean defaultValue) {
        return getChecked(key, defaultValue, Boolean.class);
    }

    public Number getNumber(String key, Number defaultValue) {
        return getChecked(key, defaultValue, Number.class);
    }

    public Number getNumber(String key, String oldKey, Number defaultValue) {
        Object value = get(key, oldKey, (Object) defaultValue);
        if (null == value) {
            return defaultValue;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("The value of " + key + " must be a Number type");
        }
        return (Number) value;
    }

    int getInt(String key, int defaultValue) {
        Number value = (Number) config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return value.intValue();
    }

    /**
     * Get and convert the value under the given key to the given type.
     *
     * @return the found value under the key - if it is of the provided type,
     *         or the provided default value if no entry for the key is found (or it's mapped to null).
     * @throws IllegalArgumentException if a value was found, but it is not of the expected type.
     */
    @Contract("_, !null, _ -> !null")
    @Nullable <V> V getChecked(String key, @Nullable V defaultValue, Class<V> expectedType) {
        Object value = config.get(key);
        return checkValue(key, defaultValue, expectedType, value);
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public <V> V get(String key, V defaultValue) {
        Object value = config.get(key);
        if (null == value) {
            return defaultValue;
        }
        return (V) value;
    }

    @SuppressWarnings("unchecked")
    @Deprecated
    public <V> V get(String newKey, String oldKey, V defaultValue) {
        Object value = config.get(newKey);
        if (null == value) {
            value = config.get(oldKey);
        }
        return null == value ? defaultValue : (V) value;
    }

    @Contract("_, _, !null, _ -> !null")
    private @Nullable <V> V getChecked(String key, String oldKey, @Nullable V defaultValue, Class<V> expectedType) {
        Object value = get(key, oldKey, null);
        return checkValue(key, defaultValue, expectedType, value);
    }

    @Contract("_, !null, _, _ -> !null; _, _, _, null -> param2")
    private @Nullable <V> V checkValue(
        String key,
        @Nullable V defaultValue,
        Class<V> expectedType,
        Object value
    ) {
        if (null == value) {
            return defaultValue;
        }
        return typedValue(key, expectedType, value);
    }

    <V> V typedValue(String key, Class<V> expectedType, Object value) {
        if (!expectedType.isInstance(value)) {
            String template = "The value of %s must be a %s.";
            String message = String.format(template, key, expectedType.getSimpleName());
            throw new IllegalArgumentException(message);
        }
        return expectedType.cast(value);
    }

    // FACTORIES

    public static CypherMapWrapper create(Map<String, Object> config) {
        if (config == null) {
            return empty();
        }
        return new CypherMapWrapper(config);
    }

    public static CypherMapWrapper empty() {
        return new CypherMapWrapper(Collections.emptyMap());
    }

    CypherMapWrapper withString(String key, String value) {
        HashMap<String, Object> newMap = new HashMap<>(config);
        newMap.put(key, value);
        return new CypherMapWrapper(newMap);
    }
}
