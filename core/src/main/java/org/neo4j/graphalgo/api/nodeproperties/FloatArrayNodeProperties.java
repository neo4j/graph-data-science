/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api.nodeproperties;

import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public interface FloatArrayNodeProperties extends NodeProperties {

    @Override
    float[] getFloatArray(long nodeId);

    @Override
    default double[] getDoubleArray(long nodeId) {
        float[] floatArray = getFloatArray(nodeId);

        if (floatArray == null) {
            return null;
        } else {

            double[] doubleArray = new double[floatArray.length];
            for (int i = 0; i < floatArray.length; i++) {
                doubleArray[i] = floatArray[i];
            }
            return doubleArray;
        }
    }

    @Override
    default Object getObject(long nodeId) {
        return getFloatArray(nodeId);
    }

    @Override
    default Value getValue(long nodeId) {
        var value = getFloatArray(nodeId);
        return value == null ? null : Values.floatArray(value);
    };

    @Override
    default ValueType getType() {
        return ValueType.FLOAT_ARRAY;
    };
}
