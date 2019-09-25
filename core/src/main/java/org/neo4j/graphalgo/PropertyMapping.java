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

import java.util.Collections;
import java.util.Map;

import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_AGGREGATION_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_PROPERTY_KEY;

public abstract class PropertyMapping {

    public static final PropertyMapping EMPTY_PROPERTY = new PropertyMapping.Resolved(
            -1,
            "",
            "",
            0.0,
            DeduplicationStrategy.DEFAULT);

    public final String propertyKey;
    public final String neoPropertyKey;
    public final double defaultValue;
    public final DeduplicationStrategy deduplicationStrategy;

    public PropertyMapping(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy) {
        this.propertyKey = propertyKey;
        this.neoPropertyKey = neoPropertyKey;
        this.defaultValue = defaultValue;
        this.deduplicationStrategy = deduplicationStrategy;
    }

    public static PropertyMapping fromObject(String propertyKey, Object stringOrMap) {
        if (stringOrMap instanceof String) {
            String neoPropertyKey = (String) stringOrMap;
            return fromObject(propertyKey, Collections.singletonMap(RELATIONSHIP_PROPERTIES_PROPERTY_KEY, neoPropertyKey));
        } else if (stringOrMap instanceof Map) {
            Map relPropertyMap = (Map) stringOrMap;

            final Object propertyNameValue = relPropertyMap.get(RELATIONSHIP_PROPERTIES_PROPERTY_KEY);
            if (propertyNameValue == null) {
                throw new IllegalArgumentException(String.format(
                        "Property was not set. Missing entry with key %s.",
                        RELATIONSHIP_PROPERTIES_PROPERTY_KEY));
            }
            if (!(propertyNameValue instanceof String)) {
                throw new IllegalArgumentException(String.format(
                        "Expected the property name to be of type String, but was %s.",
                        propertyNameValue.getClass().getSimpleName()));
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
                        "Expected the aggregation to be of type String, but was %s",
                        aggregationValue.getClass().getSimpleName()));
            }

            final Object defaultWeightValue = relPropertyMap.get(RELATIONSHIP_PROPERTIES_DEFAULT_VALUE_KEY);
            double defaultWeight;
            if (defaultWeightValue == null) {
                defaultWeight = HugeGraph.NO_WEIGHT;
            } else if (defaultWeightValue instanceof Number) {
                defaultWeight = ((Number) defaultWeightValue).doubleValue();
            } else {
                throw new IllegalStateException(String.format(
                        "Expected the default defaultWeightValue to be of type double, but was %s",
                        defaultWeightValue.getClass().getSimpleName()));
            }

            return PropertyMapping.of(
                    propertyKey,
                    neoPropertyKey,
                    defaultWeight,
                    deduplicationStrategy);
        } else {
            throw new IllegalStateException(String.format(
                    "Expected stringOrMap to be of type String or Map, but got %s",
                    stringOrMap.getClass().getSimpleName()));
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

    abstract PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy);

    public abstract PropertyMapping resolveWith(int propertyKeyId);

    private static final class Unresolved extends PropertyMapping {

        private Unresolved(
                String propertyKey,
                String neoPropertyKey,
                double defaultValue,
                DeduplicationStrategy deduplicationStrategy) {
            super(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
        }

        @Override
        public int propertyKeyId() {
            return StatementConstants.NO_SUCH_PROPERTY_KEY;
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
                    deduplicationStrategy());
        }
    }

    private static final class Resolved extends PropertyMapping {
        private final int propertyKeyId;

        private Resolved(
                int propertyKeyId,
                String propertyKey,
                String neoPropertyKey,
                double defaultValue,
                DeduplicationStrategy deduplicationStrategy) {
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
                    deduplicationStrategy);
        }

        @Override
        public PropertyMapping resolveWith(int propertyKeyId) {
            if (propertyKeyId != this.propertyKeyId) {
                throw new IllegalArgumentException(String.format(
                        "Different PropertyKeyIds: %d != %d",
                        this.propertyKeyId,
                        propertyKeyId));
            }
            return this;
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
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy) {
        return new Unresolved(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
    }
}
