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
import org.neo4j.gds.api.properties.PropertyValues;
import org.neo4j.values.storable.Value;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public interface GraphPropertyValues extends PropertyValues {

    default DoubleStream doubleValues() {
        throw unsupportedTypeException(ValueType.DOUBLE);
    }

    default LongStream longValues() {
        throw unsupportedTypeException(ValueType.LONG);
    }

    default Stream<double[]> doubleArrayValues() {
        throw unsupportedTypeException(ValueType.DOUBLE_ARRAY);
    }

    default Stream<float[]> floatArrayValues() {
        throw unsupportedTypeException(ValueType.FLOAT_ARRAY);
    }

    default Stream<long[]> longArrayValues() {
        throw unsupportedTypeException(ValueType.LONG_ARRAY);
    }

    Stream<?> objects();

    Stream<Value> values();

    long valueCount();
}
