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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.kernel.api.StatementConstants;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public abstract class PropertyMapping {

    public static final PropertyMapping EMPTY_PROPERTY = new PropertyMapping.Resolved(
        -1,
        "",
        "",
        0.0,
        DeduplicationStrategy.DEFAULT
    );

    private static final String RELATIONSHIP_PROPERTIES_PROPERTY_KEY = "property";
    private static final String RELATIONSHIP_PROPERTIES_AGGREGATION_KEY = "aggregation";
    private static final String RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY = "defaultValue";

    private static final double DEFAULT_PROPERTY_VALUE = Double.NaN;

    public final String propertyKey;
    public final String neoPropertyKey;
    public final double defaultValue;
    public final DeduplicationStrategy deduplicationStrategy;

    public PropertyMapping(
        String propertyKey,
        String neoPropertyKey,
        double defaultValue,
        DeduplicationStrategy deduplicationStrategy
    ) {
        this.propertyKey = propertyKey;
        this.neoPropertyKey = neoPropertyKey;
        this.defaultValue = defaultValue;
        this.deduplicationStrategy = deduplicationStrategy;
    }

    public static PropertyMapping fromObject(String propertyKey, Object stringOrMap) {
        if (stringOrMap instanceof String) {
            String neoPropertyKey = (String) stringOrMap;
            return fromObject(
                propertyKey,
                Collections.singletonMap(
                    RELATIONSHIP_PROPERTIES_PROPERTY_KEY,
                    neoPropertyKey
                )
            );
        } else if (stringOrMap instanceof Map) {
            Map relPropertyMap = (Map) stringOrMap;

            final Object propertyNameValue = relPropertyMap.get(RELATIONSHIP_PROPERTIES_PROPERTY_KEY);
            if (propertyNameValue == null) {
                throw new IllegalArgumentException(String.format(
                    "Expected a '%s', but no such entry found for '%s'.",
                    RELATIONSHIP_PROPERTIES_PROPERTY_KEY, RELATIONSHIP_PROPERTIES_PROPERTY_KEY
                ));
            }
            if (!(propertyNameValue instanceof String)) {
                throw new IllegalArgumentException(String.format(
                    "Expected the value of '%s' to be of type String, but was '%s'.",
                    RELATIONSHIP_PROPERTIES_PROPERTY_KEY, propertyNameValue.getClass().getSimpleName()
                ));
            }
            String neoPropertyKey = (String) propertyNameValue;

            final Object aggregationValue = relPropertyMap.get(RELATIONSHIP_PROPERTIES_AGGREGATION_KEY);
            DeduplicationStrategy deduplicationStrategy;
            if (aggregationValue == null) {
                deduplicationStrategy = DeduplicationStrategy.DEFAULT;
            } else if (aggregationValue instanceof String) {
                deduplicationStrategy = DeduplicationStrategy.lookup(((String) aggregationValue).toUpperCase());
            } else {
                throw new IllegalStateException(String.format(
                    "Expected the value of '%s' to be of type String, but was '%s'",
                    RELATIONSHIP_PROPERTIES_AGGREGATION_KEY, aggregationValue.getClass().getSimpleName()
                ));
            }

            final Object defaultPropertyValue = relPropertyMap.get(RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY);
            double defaultProperty;
            if (defaultPropertyValue == null) {
                defaultProperty = HugeGraph.NO_PROPERTY_VALUE;
            } else if (defaultPropertyValue instanceof Number) {
                defaultProperty = ((Number) defaultPropertyValue).doubleValue();
            } else {
                throw new IllegalStateException(String.format(
                    "Expected the value of '%s' to be of type double, but was '%s'",
                    RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY, defaultPropertyValue.getClass().getSimpleName()
                ));
            }

            return PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultProperty,
                deduplicationStrategy
            );
        } else {
            throw new IllegalStateException(String.format(
                "Expected stringOrMap to be of type String or Map, but got %s",
                stringOrMap.getClass().getSimpleName()
            ));
        }
    }

    /**
     * property key in the result map Graph.nodeProperties(`propertyKey`)
     */
    public String propertyKey() {
        return propertyKey;
    }

    /**
     * property name in the graph (a:Node {`propertyKey`:xyz})
     */
    public String neoPropertyKey() {
        return neoPropertyKey;
    }

    public double defaultValue() {
        return defaultValue;
    }

    public DeduplicationStrategy deduplicationStrategy() {
        return deduplicationStrategy;
    }

    /**
     * Property identifier from Neo4j token store
     */
    public abstract int propertyKeyId();

    public boolean hasValidName() {
        return neoPropertyKey != null && !neoPropertyKey.isEmpty();
    }

    public boolean exists() {
        return propertyKeyId() != StatementConstants.NO_SUCH_PROPERTY_KEY;
    }

    public PropertyMapping withDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
        if (this.deduplicationStrategy != DeduplicationStrategy.DEFAULT) {
            return this;
        }
        return copyWithDeduplicationStrategy(deduplicationStrategy);
    }

    public Map.Entry<String, Object> toObject(boolean includeAggregation) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(RELATIONSHIP_PROPERTIES_PROPERTY_KEY, neoPropertyKey);
        value.put(RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY, defaultValue);
        if (includeAggregation) {
            value.put(RELATIONSHIP_PROPERTIES_AGGREGATION_KEY, deduplicationStrategy.name());
        }
        return new AbstractMap.SimpleImmutableEntry<>(propertyKey, value);
    }

    abstract PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy);

    public abstract PropertyMapping resolveWith(int propertyKeyId);

    private static final class Unresolved extends PropertyMapping {

        private Unresolved(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            super(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
        }

        @Override
        public int propertyKeyId() {
            throw new UnsupportedOperationException("Unresolved mapping has no propertyKeyId.");
        }

        @Override
        PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
            return new Unresolved(propertyKey(), neoPropertyKey(), defaultValue(), deduplicationStrategy);
        }

        @Override
        public PropertyMapping resolveWith(int propertyKeyId) {
            return new Resolved(
                propertyKeyId,
                propertyKey(),
                neoPropertyKey(),
                defaultValue(),
                deduplicationStrategy()
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Unresolved that = (Unresolved) o;
            return propertyKey.equals(that.propertyKey) &&
                   neoPropertyKey.equals(that.neoPropertyKey) &&
                   deduplicationStrategy == that.deduplicationStrategy &&
                   Double.compare(that.defaultValue, defaultValue) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
        }
    }

    private static final class Resolved extends PropertyMapping {
        private final int propertyKeyId;

        private Resolved(
            int propertyKeyId,
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            super(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public int propertyKeyId() {
            return propertyKeyId;
        }

        @Override
        PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
            return new Resolved(
                propertyKeyId,
                propertyKey(),
                neoPropertyKey(),
                defaultValue(),
                deduplicationStrategy
            );
        }

        @Override
        public PropertyMapping resolveWith(int propertyKeyId) {
            if (propertyKeyId != this.propertyKeyId) {
                throw new IllegalArgumentException(String.format(
                    "Different PropertyKeyIds: %d != %d",
                    this.propertyKeyId,
                    propertyKeyId
                ));
            }
            return this;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Resolved that = (Resolved) o;
            return propertyKeyId == that.propertyKeyId &&
                   propertyKey.equals(that.propertyKey) &&
                   neoPropertyKey.equals(that.neoPropertyKey) &&
                   deduplicationStrategy == that.deduplicationStrategy &&
                   Double.compare(that.defaultValue, defaultValue) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(propertyKeyId, propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
        }
    }

    /**
     * Creates a PropertyMapping. The given property key is also used for internal reference.
     */
    public static PropertyMapping of(String neoPropertyKey, double defaultValue) {
        return of(neoPropertyKey, neoPropertyKey, defaultValue, DeduplicationStrategy.DEFAULT);
    }

    public static PropertyMapping of(String propertyKey, String neoPropertyKey, double defaultValue) {
        return of(propertyKey, neoPropertyKey, defaultValue, DeduplicationStrategy.DEFAULT);
    }

    public static PropertyMapping of(
        String propertyKey,
        double defaultValue,
        DeduplicationStrategy deduplicationStrategy
    ) {
        return new Unresolved(propertyKey, propertyKey, defaultValue, deduplicationStrategy);
    }

    public static PropertyMapping of(
        String propertyKey,
        DeduplicationStrategy deduplicationStrategy
    ) {
        return new Unresolved(propertyKey, propertyKey, DEFAULT_PROPERTY_VALUE, deduplicationStrategy);
    }

    public static PropertyMapping of(
        String propertyKey,
        String neoPropertyKey,
        double defaultValue,
        DeduplicationStrategy deduplicationStrategy
    ) {
        return new Unresolved(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
    }
}
