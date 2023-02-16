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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.AsJavaObject;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wrapper around configuration options map
 */
public final class ValueMapWrapper implements CypherMapAccess {

    public static ValueMapWrapper create(@Nullable MapValue config) {
        if (config == null || config.isEmpty()) {
            return empty();
        }
        Map<String, AnyValue> configMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        config.foreach((key, value) -> {
            if (value != null && value != Values.NO_VALUE) {
                configMap.put(key, value);
            }
        });
        return new ValueMapWrapper(configMap);
    }

    public static ValueMapWrapper empty() {
        return new ValueMapWrapper(Map.of());
    }

    private final Map<String, AnyValue> config;

    private ValueMapWrapper(Map<String, AnyValue> config) {
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
        AnyValue value = config.get(key);
        // Cypher always uses longs, so we have to downcast them to ints
        if (value instanceof IntegralValue) {
            return Math.toIntExact(((IntegralValue) value).longValue());
        }
        return typedValue(key, Integer.class, value);
    }

    @Override
    public <V> @NotNull V typedValue(String key, Class<V> expectedType) {
        return typedValue(key, expectedType, config.get(key));
    }

    private static <V> V typedValue(String key, Class<V> expectedType, AnyValue value) {
        var mapper = new CastMapper<>(key, expectedType);
        return value.map(mapper);
    }

    @Override
    public Map<String, Object> toMap() {
        return new HashMap<>(config);
    }

    private static final class CastMapper<T> implements PartialValueMapper<T> {
        private final String key;
        private final Class<T> expectedType;

        private CastMapper(String key, Class<T> expectedType) {
            this.key = key;
            this.expectedType = expectedType;
        }

        @Override
        public T mapIntegral(IntegralValue value) {
            if (Integer.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(Math.toIntExact(value.longValue()));
            }
            if (Long.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(value.longValue());
            }
            if (Number.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(value.doubleValue());
            }
            return PartialValueMapper.super.mapIntegral(value);
        }

        @Override
        public T mapFloatingPoint(FloatingPointValue value) {
            if (Number.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(value.longValue());
            }
            return PartialValueMapper.super.mapFloatingPoint(value);
        }

        @Override
        public T mapBoolean(BooleanValue value) {
            if (Boolean.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(value.booleanValue());
            }
            return PartialValueMapper.super.mapBoolean(value);
        }

        @Override
        public T mapText(TextValue value) {
            if (String.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(value.stringValue());
            }
            return PartialValueMapper.super.mapText(value);
        }

        @Override
        public T mapTextArray(TextArray value) {
            if (List.class.isAssignableFrom(expectedType)) {
                return expectedType.cast(List.of(value.asObject()));
            }
            return PartialValueMapper.super.mapTextArray(value);
        }

        @Override
        public T mapSequence(SequenceValue value) {
            if (List.class.isAssignableFrom(expectedType)) {
                var length = value.length();
                var list = new ArrayList<>(length);
                for (var i = 0; i < length; i++) {
                    list.add(value.value(i).map(AsJavaObject.instance()));
                }
                return expectedType.cast(list);
            }

            String message = String.format(
                Locale.ENGLISH,
                "The value of `%s` must be of type `%s` but was `List`.",
                key,
                expectedType.getSimpleName()
            );
            throw new IllegalArgumentException(message);

        }

        @Override
        public T unsupported(AnyValue value) {
            if (expectedType == Object.class && value instanceof Value) {
                return expectedType.cast(((Value) value).asObject());
            }

            String message = String.format(
                Locale.ENGLISH,
                "The value of `%s` must be of type `%s` but was `%s`.",
                key,
                expectedType.getSimpleName(),
                value.getTypeName()
            );
            throw new IllegalArgumentException(message);
        }
    }
}
