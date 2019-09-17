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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedLongDoubleMap;
import org.neo4j.kernel.api.StatementConstants;

public final class HugeNodePropertiesBuilder {

    private final double defaultValue;
    private final int propertyId;
    private final PagedLongDoubleMap properties;
    private final String propertyKey;

    public static HugeNodePropertiesBuilder of(
            long numberOfNodes,
            AllocationTracker tracker,
            double defaultValue,
            int propertyId,
            String propertyKey) {
        assert propertyId != StatementConstants.NO_SUCH_PROPERTY_KEY;
        PagedLongDoubleMap properties = PagedLongDoubleMap.of(numberOfNodes, tracker);
        return new HugeNodePropertiesBuilder(defaultValue, propertyId, properties, propertyKey);
    }

    private HugeNodePropertiesBuilder(
            final double defaultValue,
            final int propertyId,
            final PagedLongDoubleMap properties,
            final String propertyKey) {
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
        this.properties = properties;
        this.propertyKey = propertyKey;
    }

    double defaultValue() {
        return defaultValue;
    }

    int propertyId() {
        return propertyId;
    }

    String propertyKey() {
        return propertyKey;
    }

    public void set(long index, double value) {
        properties.put(index, value);
    }

    public WeightMapping build() {
        return new NodePropertyMap(properties, defaultValue, propertyId);
    }
}
