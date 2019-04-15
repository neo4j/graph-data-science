/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.api.StatementConstants;

final class HugeNodePropertiesBuilder {

    private final double defaultValue;
    private final int propertyId;
    private final PagedPropertyMap properties;

    public static HugeNodePropertiesBuilder of(
            long numberOfNodes,
            AllocationTracker tracker,
            double defaultValue,
            int propertyId) {
        assert propertyId != StatementConstants.NO_SUCH_PROPERTY_KEY;
        PagedPropertyMap properties = PagedPropertyMap.of(numberOfNodes, tracker);
        return new HugeNodePropertiesBuilder(defaultValue, propertyId, properties);
    }

    private HugeNodePropertiesBuilder(
            final double defaultValue,
            final int propertyId,
            final PagedPropertyMap properties) {
        this.defaultValue = defaultValue;
        this.propertyId = propertyId;
        this.properties = properties;
    }

    double defaultValue() {
        return defaultValue;
    }

    int propertyId() {
        return propertyId;
    }

    void set(long index, double value) {
        properties.put(index, value);
    }

    HugeWeightMapping build() {
        return new HugeNodePropertyMap(properties, defaultValue, propertyId);
    }
}
