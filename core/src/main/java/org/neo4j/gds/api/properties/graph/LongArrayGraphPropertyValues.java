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
package org.neo4j.gds.api.properties.graph;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.stream.Stream;

public interface LongArrayGraphPropertyValues extends GraphPropertyValues {

    @Override
    Stream<long[]> longArrayValues();

    @Override
    default Stream<long[]> objects() {
        return longArrayValues();
    }

    @Override
    default Stream<Value> values() {
        return longArrayValues().map(value -> value == null ? null : Values.longArray(value));
    }

    @Override
    default ValueType valueType() {
        return ValueType.LONG_ARRAY;
    }
}
