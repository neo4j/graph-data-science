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
package org.neo4j.gds;

import org.neo4j.gds.api.DefaultValue;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public record PropertyMapping(Key propertyKey, DefaultValue defaultValue, Aggregation aggregation) {
    public PropertyMapping {
        validateProperties(propertyKey.internal, propertyKey.external, aggregation);
    }

    public record Key(String internal, String external) {
        public static Key of(String internal, String external) {
            return new Key(internal, external);
        }
        public static Key simple(String propertyKey) {
            return new Key(propertyKey, propertyKey);
        }
    }

    public static final String PROPERTY_KEY = "property";
    public static final String DEFAULT_VALUE_KEY = "defaultValue";

    public String externalPropertyKey() {
        return propertyKey.external;
    }

    public String internalPropertyKey() {
        return propertyKey.internal;
    }

    private void validateProperties(String internalPropertyKey, String externalPropertyKey, Aggregation aggregation) {
        if (externalPropertyKey.equals(ElementProjection.PROJECT_ALL) && aggregation != Aggregation.COUNT) {
            throw new IllegalArgumentException("A '*' property key can only be used in combination with count aggregation.");
        }
        if (internalPropertyKey.isEmpty()) {
            throw new IllegalArgumentException("Property key must not be empty.");
        }
    }

    public static PropertyMapping fromObject(String internalPropertyKey, Object stringOrMap) {
        if (stringOrMap instanceof String externalPropertyKey) {
            return fromObject(
                internalPropertyKey,
                Collections.singletonMap(
                    PROPERTY_KEY,
                    externalPropertyKey
                )
            );
        } else if (stringOrMap instanceof Map) {
            var propertyMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            //noinspection unchecked
            propertyMap.putAll((Map<String, Object>) stringOrMap);
            Object propertyNameValue = propertyMap.getOrDefault(PROPERTY_KEY, internalPropertyKey);
            if (!(propertyNameValue instanceof String externalPropertyKey)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Expected the value of '%s' to be of type String, but was '%s'.",
                    PROPERTY_KEY, propertyNameValue.getClass().getSimpleName()
                ));
            }

            Object aggregationValue = propertyMap.get(RelationshipProjection.AGGREGATION_KEY);
            Aggregation aggregation = switch (aggregationValue) {
                case null -> Aggregation.DEFAULT;
                case String aggregationValueString -> Aggregation.parse(aggregationValueString);
                default -> throw new IllegalStateException(formatWithLocale(
                    "Expected the value of '%s' to be of type String, but was '%s'",
                    RelationshipProjection.AGGREGATION_KEY, aggregationValue.getClass().getSimpleName()
                ));
            };

            Object defaultValue = propertyMap.get(DEFAULT_VALUE_KEY);
            boolean isUserDefined = propertyMap.containsKey(DEFAULT_VALUE_KEY);
            return PropertyMapping.of(
                Key.of(internalPropertyKey, externalPropertyKey),
                DefaultValue.of(defaultValue, isUserDefined),
                aggregation
            );
        } else {
            throw new IllegalStateException(formatWithLocale(
                "Expected stringOrMap to be of type String or Map, but got %s",
                stringOrMap.getClass().getSimpleName()
            ));
        }
    }

    public Map.Entry<String, Object> toObject(boolean includeAggregation) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(PROPERTY_KEY, propertyKey.external);
        value.put(DEFAULT_VALUE_KEY, defaultValue.getObject());
        if (includeAggregation) {
            value.put(RelationshipProjection.AGGREGATION_KEY, aggregation.name());
        }
        return new AbstractMap.SimpleImmutableEntry<>(propertyKey.internal, value);
    }

    PropertyMapping setNonDefaultAggregation(Aggregation aggregation) {
        if (aggregation == Aggregation.DEFAULT || aggregation() != Aggregation.DEFAULT) {
            return this;
        }
        return new PropertyMapping(propertyKey, defaultValue, aggregation);
    }

    public static PropertyMapping of(Key propertyKey) {
        return new PropertyMapping(propertyKey, DefaultValue.DEFAULT, Aggregation.DEFAULT);
    }

    public static PropertyMapping of(Key propertyKey, DefaultValue defaultValue) {
        return new PropertyMapping(propertyKey, defaultValue,  Aggregation.DEFAULT);
    }

    public static PropertyMapping of(Key propertyKey, Aggregation aggregation) {
        return new PropertyMapping(propertyKey, DefaultValue.DEFAULT,  aggregation);
    }

    public static PropertyMapping of(Key propertyKey, DefaultValue defaultValue, Aggregation aggregation) {
        return new PropertyMapping(propertyKey, defaultValue, aggregation);
    }
}
