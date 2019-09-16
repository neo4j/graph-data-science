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
import org.neo4j.kernel.api.StatementConstants;

public abstract class PropertyMapping {

    // property name in the result map Graph.nodeProperties( <propertyName> )
    public final String propertyKey;
    // property name in the neo4j graph (a:Node {<propertyKey>:xyz})
    public final String neoPropertyKey;
    public final double defaultValue;
    public final DeduplicationStrategy deduplicationStrategy;

    public PropertyMapping(String propertyKey, String propertyKeyInGraph, double defaultValue) {
        this(propertyKey, propertyKeyInGraph, defaultValue, DeduplicationStrategy.DEFAULT);
    }

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

    /**
     * property name in the result map Graph.nodeProperties(`propertyName`)
     */
    public String propertyIdentifier() {
        return propertyIdentifier;
    }

    /**
     * property name in the graph (a:Node {`propertyKey`:xyz})
     */
    public String propertyNameInGraph() {
        return propertyNameInGraph;
    }

    public double defaultValue() {
        return defaultValue;
    }

    public DeduplicationStrategy deduplicationStrategy() {
        return deduplicationStrategy;
    }

    public abstract int propertyKeyId();

    public boolean hasValidName() {
        return propertyNameInGraph != null && !propertyNameInGraph.isEmpty();
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
                String propertyIdentifier,
                String propertyNameInGraph,
                double defaultValue,
                DeduplicationStrategy deduplicationStrategy) {
            super(propertyIdentifier, propertyNameInGraph, defaultValue, deduplicationStrategy);
        }

        @Override
        public int propertyKeyId() {
            return StatementConstants.NO_SUCH_PROPERTY_KEY;
        }

        @Override
        PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
            return new Unresolved(propertyIdentifier(), propertyNameInGraph(), defaultValue(), deduplicationStrategy);
        }

        @Override
        public PropertyMapping resolveWith(int propertyKeyId) {
            return new Resolved(propertyKeyId, propertyIdentifier(), propertyNameInGraph(), defaultValue(), deduplicationStrategy());
        }
    }

    private static final class Resolved extends PropertyMapping {
        private final int propertyKeyId;

        private Resolved(
                int propertyKeyId,
                String propertyIdentifier,
                String propertyNameInGraph,
                double defaultValue,
                DeduplicationStrategy deduplicationStrategy) {
            super(propertyIdentifier, propertyNameInGraph, defaultValue, deduplicationStrategy);
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public int propertyKeyId() {
            return propertyKeyId;
        }

        @Override
        PropertyMapping copyWithDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
            return new Resolved(propertyKeyId, propertyIdentifier(), propertyNameInGraph(), defaultValue(), deduplicationStrategy);
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


    public static PropertyMapping of(String propertyIdentifier, String propertyNameInGraph, double defaultValue) {
        return of(propertyIdentifier, propertyNameInGraph, defaultValue, DeduplicationStrategy.DEFAULT);
    }

    public static PropertyMapping of(
            String propertyIdentifier,
            String propertyNameInGraph,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy) {
        return new Unresolved(propertyIdentifier, propertyNameInGraph, defaultValue, deduplicationStrategy);
    }
}
