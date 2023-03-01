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
package org.neo4j.gds.api.properties.nodes;

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.ValueConversion;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.OptionalLong;
import java.util.stream.LongStream;

public interface LongNodePropertyValues extends NodePropertyValues {

    @Override
    long longValue(long nodeId);

    @Override
    default Object getObject(long nodeId) {
        return longValue(nodeId);
    }

    @Override
    default Value value(long nodeId) {
        return Values.longValue(longValue(nodeId));
    }

    @Override
    default ValueType valueType() {
        return ValueType.LONG;
    }

    @Override
    default double doubleValue(long nodeId) {
        long value = longValue(nodeId);
        if (value == DefaultValue.LONG_DEFAULT_FALLBACK) {
            return DefaultValue.DOUBLE_DEFAULT_FALLBACK;
        }
        return ValueConversion.exactLongToDouble(value);
    }

    @Override
    default OptionalLong getMaxLongPropertyValue() {
        return LongStream
            .range(0, valuesStored())
            .parallel()
            .map(this::longValue)
            .max();
    }
}
