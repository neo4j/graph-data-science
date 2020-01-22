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
package org.neo4j.graphalgo;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.HugeGraph;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@ValueClass
public abstract class PropertyMapping {

    public static final double DEFAULT_FALLBACK_VALUE = Double.NaN;

    public static final String PROPERTY_KEY = "property";
    public static final String DEFAULT_VALUE_KEY = "defaultValue";

    /**
     * property key in the result map Graph.nodeProperties(`propertyKey`)
     */
    public abstract @Nullable String propertyKey();

    /**
     * property name in the graph (a:Node {`propertyKey`:xyz})
     */
    @Value.Default
    public @Nullable String neoPropertyKey() {
        return propertyKey();
    }

    @Value.Default
    public double defaultValue() {
        return DEFAULT_FALLBACK_VALUE;
    }

    @Value.Default
    public Aggregation aggregation() {
        return Aggregation.DEFAULT;
    }

    public static PropertyMapping fromObject(String propertyKey, Object stringOrMap) {
        if (stringOrMap instanceof String) {
            String neoPropertyKey = (String) stringOrMap;
            return fromObject(
                propertyKey,
                Collections.singletonMap(
                    PROPERTY_KEY,
                    neoPropertyKey
                )
            );
        } else if (stringOrMap instanceof Map) {
            Map<String, Object> relPropertyMap = (Map) stringOrMap;
            Object propertyNameValue = relPropertyMap.getOrDefault(PROPERTY_KEY, propertyKey);
            if (!(propertyNameValue instanceof String)) {
                throw new IllegalArgumentException(String.format(
                    "Expected the value of '%s' to be of type String, but was '%s'.",
                    PROPERTY_KEY, propertyNameValue.getClass().getSimpleName()
                ));
            }
            String neoPropertyKey = (String) propertyNameValue;

            final Object aggregationValue = relPropertyMap.get(RelationshipProjection.AGGREGATION_KEY);
            Aggregation aggregation;
            if (aggregationValue == null) {
                aggregation = Aggregation.DEFAULT;
            } else if (aggregationValue instanceof String) {
                aggregation = Aggregation.lookup(((String) aggregationValue).toUpperCase());
            } else {
                throw new IllegalStateException(String.format(
                    "Expected the value of '%s' to be of type String, but was '%s'",
                    RelationshipProjection.AGGREGATION_KEY, aggregationValue.getClass().getSimpleName()
                ));
            }

            final Object defaultPropertyValue = relPropertyMap.get(DEFAULT_VALUE_KEY);
            double defaultProperty;
            if (defaultPropertyValue == null) {
                defaultProperty = HugeGraph.NO_PROPERTY_VALUE;
            } else if (defaultPropertyValue instanceof Number) {
                defaultProperty = ((Number) defaultPropertyValue).doubleValue();
            } else {
                throw new IllegalStateException(String.format(
                    "Expected the value of '%s' to be of type double, but was '%s'",
                    DEFAULT_VALUE_KEY, defaultPropertyValue.getClass().getSimpleName()
                ));
            }

            return PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultProperty,
                aggregation
            );
        } else {
            throw new IllegalStateException(String.format(
                "Expected stringOrMap to be of type String or Map, but got %s",
                stringOrMap.getClass().getSimpleName()
            ));
        }
    }

    public boolean hasValidName() {
        String key = neoPropertyKey();
        return key != null && !key.isEmpty();
    }

    public boolean exists() {
        return false;
    }

    public Map.Entry<String, Object> toObject(boolean includeAggregation) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(PROPERTY_KEY, neoPropertyKey());
        value.put(DEFAULT_VALUE_KEY, defaultValue());
        if (includeAggregation) {
            value.put(RelationshipProjection.AGGREGATION_KEY, aggregation().name());
        }
        return new AbstractMap.SimpleImmutableEntry<>(propertyKey(), value);
    }

    public PropertyMapping setNonDefaultAggregation(Aggregation aggregation) {
        if (aggregation == Aggregation.DEFAULT || aggregation() != Aggregation.DEFAULT) {
            return this;
        }
        return ((ImmutablePropertyMapping) this).withAggregation(aggregation);
    }

    public final ResolvedPropertyMapping resolveWith(int propertyKeyId) {
        String propertyKey = Objects.requireNonNull(propertyKey(), "propertyKey must not be null");
        String neoPropertyKey = Objects.requireNonNull(neoPropertyKey(), "neoPropertyKey must not be null");
        return ImmutableResolvedPropertyMapping
            .builder()
            .propertyKey(propertyKey)
            .neoPropertyKey(neoPropertyKey)
            .propertyKeyId(propertyKeyId)
            .defaultValue(defaultValue())
            .aggregation(aggregation())
            .build();
    }

    /**
     * Creates a PropertyMapping. The given property key is also used for internal reference.
     */
    public static PropertyMapping of(String neoPropertyKey, double defaultValue) {
        return ImmutablePropertyMapping
            .builder()
            .propertyKey(neoPropertyKey)
            .defaultValue(defaultValue)
            .build();
    }

    public static PropertyMapping of(String propertyKey, String neoPropertyKey, double defaultValue) {
        return ImmutablePropertyMapping
            .builder()
            .propertyKey(propertyKey)
            .neoPropertyKey(neoPropertyKey)
            .defaultValue(defaultValue)
            .build();
    }

    public static PropertyMapping of(
        String propertyKey,
        double defaultValue,
        Aggregation aggregation
    ) {
        return ImmutablePropertyMapping
            .builder()
            .propertyKey(propertyKey)
            .defaultValue(defaultValue)
            .aggregation(aggregation)
            .build();
    }

    public static PropertyMapping of(
        String propertyKey,
        Aggregation aggregation
    ) {
        return ImmutablePropertyMapping
            .builder()
            .propertyKey(propertyKey)
            .aggregation(aggregation)
            .build();
    }

    public static PropertyMapping of(
        String propertyKey,
        String neoPropertyKey,
        double defaultValue,
        Aggregation aggregation
    ) {
        return ImmutablePropertyMapping.of(propertyKey, neoPropertyKey, defaultValue, aggregation);
    }

    public static PropertyMapping of(ResolvedPropertyMapping resolvedPropertyMapping) {
        return of(
            resolvedPropertyMapping.propertyKey(),
            resolvedPropertyMapping.neoPropertyKey(),
            resolvedPropertyMapping.defaultValue(),
            resolvedPropertyMapping.aggregation()
        );
    }
}
