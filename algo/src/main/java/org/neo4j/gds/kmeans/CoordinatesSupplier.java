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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.function.Supplier;

public final class CoordinatesSupplier implements Supplier<Coordinate> {

    private final NodePropertyValues values;
    private final  int dimensions;

    public CoordinatesSupplier(NodePropertyValues nodePropertyValues, int dimensions, int k) {
        this.values = nodePropertyValues;
        this.dimensions = dimensions;
    }

    @Override
    public Coordinate get() {
        if (values.valueType() == ValueType.FLOAT_ARRAY) {
            return new FloatArrayCoordinate(dimensions, values);
         } else if (values.valueType() == ValueType.DOUBLE_ARRAY) {
            return new DoubleArrayCoordinate(dimensions, values);
        }else if (values.valueType()== ValueType.DOUBLE){
            return  new ScalarCoordinate(values);
        }
        throw new IllegalArgumentException("Incorrect data type");
    }
}
