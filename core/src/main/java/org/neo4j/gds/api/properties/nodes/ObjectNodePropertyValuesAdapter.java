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

import org.neo4j.gds.collections.ha.HugeObjectArray;

final class ObjectNodePropertyValuesAdapter {

    private ObjectNodePropertyValuesAdapter() {}

    public static NodePropertyValues adapt(HugeObjectArray<?> objectArray) {
        var cls = objectArray.elementClass();
        if (cls == float[].class) {
            return new FloatArrayNodePropertyValues() {
                @Override
                public float[] floatArrayValue(long nodeId) {
                    return (float[]) objectArray.get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return objectArray.size();
                }
            };
        }
        if (cls == double[].class) {
            return new DoubleArrayNodePropertyValues() {
                @Override
                public double[] doubleArrayValue(long nodeId) {
                    return (double[]) objectArray.get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return objectArray.size();
                }
            };
        }
        if (cls == long[].class) {
            return new LongArrayNodePropertyValues() {
                @Override
                public long[] longArrayValue(long nodeId) {
                    return (long[]) objectArray.get(nodeId);
                }

                @Override
                public long nodeCount() {
                    return objectArray.size();
                }
            };
        }
        throw new UnsupportedOperationException("This HugeObjectArray can not be converted to node properties.");
    }

}
