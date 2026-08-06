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

/**
 * <p>Unlike a plain array property, a vector property has one dimension shared by every node, known up
 * front rather than derived from the data.
 */
public interface VectorNodePropertyValues extends NodePropertyValues {

    /**
     * @return the dimension shared by every vector of this property.
     */
    int vectorDimension();

    final class FloatVector implements FloatVectorNodePropertyValues {
        private final NodePropertyValues internal;
        private final int dimension;

        private FloatVector(NodePropertyValues internal, int dimension) {
            this.internal = internal;
            this.dimension = dimension;
        }

        @Override
        public int vectorDimension() {
            return dimension;
        }

        @Override
        public float[] floatArrayValue(long nodeId) {
            return internal.floatArrayValue(nodeId);
        }

        @Override
        public long nodeCount() {
            return internal.nodeCount();
        }

        @Override
        public boolean hasValue(long nodeId) {
            return internal.hasValue(nodeId);
        }
    }

    final class DoubleVector implements DoubleVectorNodePropertyValues {
        private final NodePropertyValues internal;
        private final int dimension;

        private DoubleVector(NodePropertyValues internal, int dimension) {
            this.internal = internal;
            this.dimension = dimension;
        }

        @Override
        public int vectorDimension() {
            return dimension;
        }

        @Override
        public double[] doubleArrayValue(long nodeId) {
            return internal.doubleArrayValue(nodeId);
        }

        @Override
        public long nodeCount() {
            return internal.nodeCount();
        }

        @Override
        public boolean hasValue(long nodeId) {
            return internal.hasValue(nodeId);
        }
    }

}
