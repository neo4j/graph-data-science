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

public class PropertyMapping {

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

    public PropertyMapping withDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
        if (this.deduplicationStrategy == DeduplicationStrategy.DEFAULT) {
            return new PropertyMapping(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
        }
        return this;
    }

    public KernelPropertyMapping toKernelMapping(int propertyKeyId) {
        return new KernelPropertyMapping(propertyKey, propertyKeyId, defaultValue,
                neoPropertyKey, deduplicationStrategy);
    }

    public static PropertyMapping of(String propertyKey, String neoPropertyKey, double defaultValue) {
        return new PropertyMapping(propertyKey, neoPropertyKey, defaultValue);
    }

    public static PropertyMapping of(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy) {
        return new PropertyMapping(propertyKey, neoPropertyKey, defaultValue, deduplicationStrategy);
    }

    public static PropertyMapping of(
            String propertyName,
            String propertyKeyInGraph,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy) {
        return new PropertyMapping(propertyName, propertyKeyInGraph, defaultValue, deduplicationStrategy);
    }
}
