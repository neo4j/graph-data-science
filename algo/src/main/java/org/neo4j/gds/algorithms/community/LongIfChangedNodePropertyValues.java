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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.FilteredNodePropertyValuesMarker;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LongIfChangedNodePropertyValues implements LongNodePropertyValues, FilteredNodePropertyValuesMarker {

    private final NodePropertyValues seedProperties;
    private final NodePropertyValues newProperties;

    public static LongNodePropertyValues of(NodeProperty seedProperty, LongNodePropertyValues newProperties) {
        var propertyState = seedProperty.propertyState();
        if (propertyState == PropertyState.PERSISTENT) {
            NodePropertyValues seedProperties = seedProperty.values();
            // TODO forbid doubles once we load properties with their correct type
            if (seedProperties.valueType() == ValueType.LONG || seedProperties.valueType() == ValueType.DOUBLE) {
                return new LongIfChangedNodePropertyValues(seedProperties, newProperties);
            } else {
                throw new IllegalStateException(formatWithLocale(
                    "Expected seedProperty `%s` to be of type %s, but was %s",
                    seedProperty,
                    ValueType.LONG,
                    seedProperties.valueType()
                ));
            }
        } else {
            return newProperties;
        }
    }

    private LongIfChangedNodePropertyValues(
        NodePropertyValues seedProperties,
        NodePropertyValues newProperties
    ) {
        this.seedProperties = seedProperties;
        this.newProperties = newProperties;
    }

    /**
     * Returning Long.MIN_VALUE indicates that the value should not be written to Neo4j.
     * <p>
     * The filter is applied in the latest stage before writing to Neo4j.
     * Since the wrapped node properties may have additional logic in longValue(),
     * we need to check if they already filtered the value. Only in the case
     * where the wrapped properties pass on the value, we can apply a filter.
     */
    @Override
    public long longValue(long nodeId) {
        var seedValue = seedProperties.longValue(nodeId);
        var writeValue = newProperties.longValue(nodeId);

        return (seedValue != writeValue) ? writeValue : Long.MIN_VALUE;
    }

    @Override
    public Value value(long nodeId) {
        var value = longValue(nodeId);
        if (value == Long.MIN_VALUE) {
            return null;
        }
        return Values.longValue(value);
    }

    @Override
    public long nodeCount() {
        return Math.max(newProperties.nodeCount(), seedProperties.nodeCount());
    }

    @Override
    public boolean hasValue(long nodeId) {
        long seedValue = seedProperties.longValue(nodeId);
        long writeValue = newProperties.longValue(nodeId);
        return seedValue == Long.MIN_VALUE || (seedValue != writeValue);

    }
}
