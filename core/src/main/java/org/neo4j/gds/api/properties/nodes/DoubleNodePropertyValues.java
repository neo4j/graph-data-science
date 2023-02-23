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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.OptionalDouble;
import java.util.stream.LongStream;

public interface DoubleNodePropertyValues extends NodePropertyValues {

    @Override
    double doubleValue(long nodeId);

    @Override
    default Object getObject(long nodeId) {
        return doubleValue(nodeId);
    }

    @Override
    default Value value(long nodeId) {
        var value = doubleValue(nodeId);
        return Double.isNaN(value) ? null : Values.doubleValue(value);
    }

    @Override
    default ValueType valueType() {
        return ValueType.DOUBLE;
    }

    @Override
    default int dimension() {
        return 1;
    }

    @Override
    default OptionalDouble getMaxDoublePropertyValue() {
        return LongStream
            .range(0, size())
            .parallel()
            .mapToDouble(this::doubleValue)
            .max();
    }
}
