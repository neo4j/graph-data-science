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
package org.neo4j.gds.nodeproperties;

import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LongIfChangedNodePropertyValues implements LongNodePropertyValues {

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

    @Override
    public long longValue(long nodeId) {
        return newProperties.longValue(nodeId);
    }

    @Override
    public Value value(long nodeId) {
        long seedValue = seedProperties.longValue(nodeId);
        long writeValue = newProperties.longValue(nodeId);

        return seedValue == Long.MIN_VALUE || (seedValue != writeValue) ? Values.longValue(writeValue) : null;
    }

    @Override
    public long size() {
        return newProperties.size();
    }
}
